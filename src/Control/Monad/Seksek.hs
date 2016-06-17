{-# LANGUAGE TemplateHaskell, GADTs, RecursiveDo, DeriveFunctor #-}
{-# OPTIONS_GHC -Wall -fno-warn-unused-binds #-}

module Control.Monad.Seksek (
  SeksekHandler(..), SeksekProgram, getInit, remote, remember, forget,
  serveSeksek, mockSeksek, initialJob, sendJob
  ) where

import Network.Beanstalk (BeanstalkServer, connectBeanstalk, useTube, watchTube,
                          putJob, reserveJob, deleteJob, buryJob, job_body,
                          job_id, JobState)
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.ByteString.Char8 as BS
import Data.Aeson (defaultOptions, Value, ToJSON, FromJSON, encode,
                   toJSON, fromJSON, decodeStrict', Result(Success))
import Data.Aeson.TH (deriveJSON)
import Data.String (fromString)
import Data.Maybe (isNothing)
import Control.Monad.Except (throwError, liftIO, runExceptT, ExceptT)
import Control.Monad
import Control.Error.Util ((??), note, hoistEither)
import Control.Monad.Operational (Program, singleton, view, ProgramViewT(..))
import Control.Arrow (first)
import Safe
-- import Debug.Trace

data SeksekRequestMeta fwd = SeksekRequestMeta {
       response_tube :: String
     , forward :: fwd
     }

data SeksekRequest req fwd = SeksekRequest {
       request_body :: req
     , metadata :: SeksekRequestMeta fwd
     }

data SeksekResponse rsp st = SeksekResponse {
      response_body :: rsp
    , forwarded :: SeksekContinuation st
    }

data SeksekContinuation st = SeksekContinuation {
      handler_id :: [Int]
    , appstate :: st
    }

$(deriveJSON defaultOptions ''SeksekRequestMeta)
$(deriveJSON defaultOptions ''SeksekRequest)
$(deriveJSON defaultOptions ''SeksekContinuation)
$(deriveJSON defaultOptions ''SeksekResponse)

data SeksekHandler inp out = SeksekHandler {tubeName :: BS.ByteString} deriving Show

call_service :: ToJSON inp => BeanstalkServer -> SeksekHandler inp out -> String -> SeksekContinuation [[Value]] -> inp -> IO ()
call_service con srv app cont i = do
    useTube con $ tubeName srv
    _ <- putJob con 0 0 100 req_body
    return ()
  where
    req_body = BSL.toStrict $ encode $ toJSON $ SeksekRequest i $ SeksekRequestMeta app cont

data Seksek a where
   Remote :: (ToJSON a, FromJSON b) => SeksekHandler a b -> a -> Seksek b
   Remember :: (ToJSON a, FromJSON a) => IO a -> Seksek a
   Forget :: IO a -> Seksek ()
--   Nested :: (FromJSON a) => SeksekProgram a -> Seksek a

remote :: (ToJSON a, FromJSON b) => SeksekHandler a b -> (a -> SeksekProgram b)
remote h = singleton . Remote h

remember :: (ToJSON a, FromJSON a) => IO a -> SeksekProgram a
remember = singleton . Remember

forget :: IO a -> SeksekProgram ()
forget = singleton . Forget

-- nested :: SeksekProgram a -> SeksekProgram a
-- nested = singleton . Nested

type SeksekProgram a = Program Seksek a

makeContinuation :: [Int] -> [[Value]] -> SeksekContinuation [[Value]]
makeContinuation = SeksekContinuation

safeSplit :: [a] -> Maybe (a, [a])
safeSplit (a:as) = Just (a,as)
safeSplit [] = Nothing

safeSplitEnd :: [a] -> Maybe ([a], a)
safeSplitEnd (a:as) = Just $ maybe ([],a) (first (a:)) $ safeSplitEnd as
safeSplitEnd [] = Nothing


data ListZipper a = ListZipper {zbefore :: [a], zafter :: [a]} deriving (Functor, Show)

(<&>) :: Functor f => f a -> (a -> b) -> f b
(<&>) = flip (<$>)

(&) :: a -> (a -> b) -> b
(&) = flip ($)

listToZipper :: [a] -> ListZipper a
listToZipper = ListZipper []

listToZipper2 :: [[a]] -> ListZipper (ListZipper a)
listToZipper2 = listToZipper . fmap listToZipper

listToZipperRight :: [a] -> ListZipper a
listToZipperRight (a:as) = ListZipper as [a]
listToZipperRight [] = ListZipper [] []

listToZipperRight2 :: [[a]] -> ListZipper (ListZipper a)
listToZipperRight2 = listToZipperRight . fmap listToZipperRight


focused :: ListZipper a -> Maybe a
focused (ListZipper _ as) = headMay as

toRight :: ListZipper a -> Maybe (a, ListZipper a)
toRight (ListZipper bs as) = safeSplit as <&> \(a, rest) -> (a, ListZipper (a:bs) rest)

toLeft :: ListZipper a -> Maybe (a, ListZipper a)
toLeft (ListZipper bs as) = safeSplit bs <&> \(b, rest) -> (b, ListZipper rest (b:as))

pushLeft :: a -> ListZipper a -> ListZipper a
pushLeft a (ListZipper bs as) = ListZipper (a:bs) as

append :: a -> ListZipper a -> ListZipper a
append a (ListZipper bs as) = ListZipper bs (as ++ [a])

modify :: ListZipper a -> (a -> a) -> ListZipper a
modify (ListZipper bs (a:as)) f = ListZipper bs (f a:as)
modify lz@(ListZipper _ []) _ = lz

zipperToList :: ListZipper a -> [a]
zipperToList (ListZipper (b:bs) as) = zipperToList $ ListZipper bs (b:as)
zipperToList (ListZipper [] as) = as

type StateZipper = ListZipper FrameZipper
type FrameZipper = ListZipper Value
type HandlerZipper = ListZipper Int

data SeksekInstruction a where
  Ask :: (ToJSON inp, FromJSON out) =>
    SeksekHandler inp out -> SeksekContinuation [[Value]] ->
    inp -> (out -> SeksekProgram a) -> SeksekInstruction a
  Tell :: a -> SeksekInstruction a
  Perform :: IO out -> (out -> Either String (SeksekInstruction a)) -> SeksekInstruction a

(?<) :: Maybe b -> a -> Either a b
(?<) = flip note

gather :: Monad m => (a -> b -> c -> m d) -> m a -> m b -> m c -> m d
gather f a b c = (\(a',b',c') -> f a' b' c') =<< (,,) <$> a <*> b <*> c

type EitherInstruction a = Either String (SeksekInstruction a)
type ExceptInstruction m a = ExceptT String m (SeksekInstruction a)

runSeksek :: Value -> SeksekContinuation [[Value]] -> SeksekProgram a -> EitherInstruction a
runSeksek resp (SeksekContinuation hid state') prog =
  let state = maybe [[resp]] (\(as, ass) -> ((as ++ [resp]):ass)) $ safeSplit state' :: [[Value]]
      statezipper = listToZipper2 state
  in stepSeksek [0] (listToZipper hid) statezipper prog


stepSeksek :: [Int] -> HandlerZipper -> StateZipper -> SeksekProgram a -> EitherInstruction a
stepSeksek curIdxs hz sz m = let
  stepFocused :: SeksekProgram a -> Int -> Int -> FrameZipper -> EitherInstruction a
  stepFocused p hid curIdx curframe =
    let
      ma >>| b = ma >> return b
      stepReplay :: FromJSON b => (b -> SeksekProgram a) -> EitherInstruction a
      stepReplay cont = do
        (next, right) <- toRight curframe ?< "The state array is too short!"
        out <- maybeFromJSON next ?< ("Failed to decode output from seksek response:" ++ show next)
        stepSeksek nextIdxs hz (putFrame right) $ cont out
      assertAfterEmpty = unless (isNothing $ toRight curframe) $ throwError "Internal Error! The 'after' list should be empty."
      state = zipperToList $ fmap zipperToList sz :: [[Value]]
      nextIdxs = (curIdx+1: tail curIdxs)
      putFrame f = modify sz (const f)
    in if curIdx < hid
    then case view p of -- Replay Mode
      Return _ -> throwError "No more steps remaining"
      Remote _ _ :>>= k -> stepReplay k
      Remember _ :>>= k -> stepReplay k
      Forget _   :>>= k -> stepSeksek nextIdxs hz sz $ k ()
    else assertAfterEmpty >>| case view p of -- Execution Mode
      Return a -> Tell a
      Remote h a :>>= k -> Ask h (makeContinuation nextIdxs state) a k
      Remember a :>>= k -> Perform a $ \out -> stepSeksek nextIdxs hz (modify sz (pushLeft $ toJSON out)) $ k out
      Forget a   :>>= k -> Perform a $ \_   -> stepSeksek nextIdxs hz sz $ k ()
  in gather (stepFocused m) (note "Handler list is too short!" (focused hz))
              (note "Internal rrror! Current index stack underflow" (headMay curIdxs))
              (note "State stack is too short!" (focused sz))

conditionProgram :: FromJSON a => (a -> SeksekProgram b) -> SeksekProgram b
conditionProgram prog = getInit >>= prog

readErr :: Read a => String -> IO (Either String a)
readErr msg = fmap (note msg . readMay) getLine

performAction :: IO a -> (a -> EitherInstruction b) -> ExceptInstruction IO b
performAction action step = liftIO action >>= (hoistEither . step)

serveSeksek :: FromJSON a => String -> String -> String -> (a -> SeksekProgram ()) -> IO ()
serveSeksek host port appname prog' = let prog = conditionProgram prog' in do
  con <- connectBeanstalk host port
  forever $ do
    _ <- watchTube con $ BS.pack appname
    job <- reserveJob con
    let resp = decodeStrict' $ job_body job :: Maybe (SeksekResponse Value [[Value]])
    result <- runExceptT $ do
      SeksekResponse rbody cont <- resp ?? "Couldn't decode a SeksekResponse from the job body"
      instruction <- hoistEither $ runSeksek rbody cont prog
      let handle (Ask handler nextCont inp _) = liftIO $ call_service con handler appname nextCont inp
          handle (Tell ()) = return ()
          handle (Perform action nextCont) = performAction action nextCont >>= handle
      handle instruction
    case result of
      Right () -> deleteJob con $ job_id job
      Left err -> putStrLn err >> buryJob con (job_id job) 0

mockSeksek' :: SeksekResponse Value [[Value]] -> SeksekProgram () -> ExceptT String IO ()
mockSeksek' (SeksekResponse resp cont) prog = do
      instruction <- hoistEither $ runSeksek resp cont prog
      let handle (Ask handler nextCont inp _) = do
            liftIO $ do
              putStrLn $ "Handler " ++ show (tubeName handler) ++ " is called with input " ++ show (encode inp)
              putStrLn $ "Along with the continuation:" ++ show (encode nextCont)
            mock <- receiveMock
            mockSeksek' (SeksekResponse mock nextCont) prog
          handle (Tell ()) = return ()
          handle (Perform action rest) = performAction action rest >>= handle
      handle instruction

mockSeksek :: FromJSON a => (a -> SeksekProgram ()) -> IO ()
mockSeksek prog = do
  result <- runExceptT $ do
    mock <- receiveMock
    let resp = SeksekResponse mock (SeksekContinuation [1] [[]])
    mockSeksek' resp $ conditionProgram prog
  case result of
    Left err -> putStrLn $ "Error! " ++ err
    Right () -> putStrLn "Congrats!"

receiveMock :: ExceptT String IO Value
receiveMock = do
  mockInp <- liftIO $ do
    putStrLn "Enter mock value:"
    BS.getLine
  decodeStrict' mockInp ?? "Unable to decode the input as JSON"


maybeFromJSON :: FromJSON a => Value -> Maybe a
maybeFromJSON val = case fromJSON val of
  Success s -> Just s
  _ -> Nothing

getInit :: FromJSON a => SeksekProgram a
getInit = remote (SeksekHandler $ fromString "") ()

initialJob :: ToJSON a => a -> BS.ByteString
initialJob a = BSL.toStrict $ encode $ SeksekResponse a $ SeksekContinuation [1] ([] :: [()])

sendJob :: ToJSON a => String -> String -> String -> a -> IO (JobState, Int)
sendJob host port appname a = do
  con <- connectBeanstalk host port
  useTube con $ fromString appname
  putJob con 0 0 100 $ initialJob a
