{-# LANGUAGE TemplateHaskell, GADTs, RecursiveDo, DeriveFunctor #-}
{-# OPTIONS_GHC -Wall -fno-warn-unused-binds #-}

module Control.Monad.Seksek (
  SeksekHandler(..), SeksekProgram, getInit, remote, remember, forget, nested,
  chain, serveSeksek, mockSeksek, initialJob, sendJob
  ) where

import Network.Beanstalk (BeanstalkServer, connectBeanstalk, useTube, watchTube,
                          putJob, reserveJob, deleteJob, buryJob, job_body,
                          job_id, JobState, disconnectBeanstalk)
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.ByteString.Char8 as BS
import Data.Aeson (defaultOptions, Value, ToJSON, FromJSON, encode,
                   toJSON, fromJSON, decodeStrict', Result(Success))
import Data.Aeson.TH (deriveJSON, SumEncoding(TwoElemArray), Options(sumEncoding))
import Data.String (fromString)
import Data.Maybe (isNothing)
import Control.Monad.Except (throwError, liftIO, runExceptT, ExceptT)
import Control.Monad (unless, (>=>), forever)
import Control.Error.Util ((??), note, hoistEither)
import Control.Monad.Operational (Program, singleton, view, ProgramViewT(..))
import qualified Control.Exception as E

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
      handler_id :: Int
    , appstate :: st
    }

$(deriveJSON defaultOptions ''SeksekRequestMeta)
$(deriveJSON defaultOptions ''SeksekRequest)
$(deriveJSON defaultOptions ''SeksekContinuation)
$(deriveJSON defaultOptions ''SeksekResponse)

data SeksekHandler inp out = SeksekHandler {tubeName :: BS.ByteString} deriving Show

call_service :: ToJSON inp => BeanstalkServer -> SeksekHandler inp out -> String -> SeksekContinuation [Value] -> inp -> IO ()
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
   Chain :: (ToJSON b, FromJSON c) => SeksekHandler b c -> (a -> b) -> SeksekProgram a -> Seksek c
   Nested :: (ToJSON a, FromJSON a) => SeksekProgram a -> Seksek a

remote :: (ToJSON a, FromJSON b) => SeksekHandler a b -> (a -> SeksekProgram b)
remote h = singleton . Remote h

remember :: (ToJSON a, FromJSON a) => IO a -> SeksekProgram a
remember = singleton . Remember

forget :: IO a -> SeksekProgram ()
forget = singleton . Forget

chain :: (ToJSON b, FromJSON c) => SeksekHandler b c -> (a -> b) -> SeksekProgram a -> SeksekProgram c
chain h f = singleton . Chain h f

nested :: (ToJSON a, FromJSON a) => SeksekProgram a -> SeksekProgram a
nested = singleton . Nested

type SeksekProgram a = Program Seksek a

makeContinuation :: Int -> [Value] -> SeksekContinuation [Value]
makeContinuation = SeksekContinuation

data ChainState = Completed | Ongoing (SeksekContinuation [Value])
$(deriveJSON (defaultOptions{sumEncoding = TwoElemArray}) ''ChainState)

safeSplit :: [a] -> Maybe (a, [a])
safeSplit (a:as) = Just (a,as)
safeSplit [] = Nothing

data ListZipper a = ListZipper {zbefore :: [a], zafter :: [a]} deriving (Functor, Show)

(<&>) :: Functor f => f a -> (a -> b) -> f b
(<&>) = flip (<$>)

listToZipper :: [a] -> ListZipper a
listToZipper = ListZipper []

toRight :: ListZipper a -> Maybe (a, ListZipper a)
toRight (ListZipper bs as) = safeSplit as <&> \(a, rest) -> (a, ListZipper (a:bs) rest)

zipperToList :: ListZipper a -> [a]
zipperToList (ListZipper (b:bs) as) = zipperToList $ ListZipper bs (b:as)
zipperToList (ListZipper [] as) = as

type FrameZipper = ListZipper Value

data SeksekInstruction a where
  Ask :: (ToJSON inp, FromJSON out) =>
    SeksekHandler inp out -> SeksekContinuation [Value] ->
    inp -> SeksekInstruction a
  Tell :: a -> SeksekInstruction a
  Perform :: IO out -> (out -> Either String (SeksekInstruction a)) -> SeksekInstruction a

(?<) :: Maybe b -> a -> Either a b
(?<) = flip note

type EitherInstruction a = Either String (SeksekInstruction a)
type ExceptInstruction m a = ExceptT String m (SeksekInstruction a)

runSeksek :: Value -> SeksekContinuation [Value] -> SeksekProgram a -> EitherInstruction a
runSeksek resp (SeksekContinuation hid state') prog =
  let state = listToZipper state'
  in replaySeksek 0 hid resp state prog

assertLast :: FrameZipper -> Either String ()
assertLast frame = unless (isNothing $ toRight frame) $
  throwError $ "Internal Error! The 'after' list should be empty." ++ show frame

replaySeksek :: Int -> Int -> Value -> FrameZipper -> SeksekProgram a -> EitherInstruction a
replaySeksek curIdx hid resp curframe p = let
      nextIdx = curIdx+1
      stepReplay :: FromJSON b => (b -> SeksekProgram a) -> EitherInstruction a
      stepReplay cont = do
        (next, right) <- toRight curframe ?< "The state array is too short!"
        out <- maybeFromJSON next ?< ("Failed to decode output from seksek response:" ++ show next ++ show curIdx)
        replaySeksek nextIdx hid resp right $ cont out
      getOneAfter :: Either String Value
      getOneAfter = case toRight curframe of
        Nothing -> throwError $ "Internal Error! The state array is too short for nested!" ++ show curframe
        Just (nextVal, nextFrame) -> case toRight nextFrame of
          Nothing -> return nextVal
          Just _  -> throwError $ "Internal Error! The state array is too long!" ++ show curframe
      stateBeforeNesting = init $ zipperToList curframe
      runNested :: ToJSON a => SeksekProgram a -> (a -> SeksekInstruction b) ->
                   (SeksekContinuation [Value] -> SeksekContinuation [Value]) -> EitherInstruction b
      runNested pN cH cR = do
        nestedCont'  <- getOneAfter
        nestedCont <- maybeFromJSON nestedCont' ?< "Internal Error! The JSON value can't be decoded as a nested continuation"
        nestedResult <- runSeksek resp nestedCont pN
        handleNested curIdx cR cH nestedResult
      unpackRemote :: FromJSON a => (a -> SeksekProgram b) -> EitherInstruction b
      unpackRemote k = do
        out <- maybeFromJSON resp ?< ("Failed to decode output from seksek response:" ++ show resp)
        executeSeksek nextIdx (zipperToList curframe ++ [resp]) $ k out
    in case compare curIdx hid of
      LT -> case view p of -- Replay Mode
        Return _ -> throwError "No more steps remaining"
        Forget _   :>>= k -> replaySeksek nextIdx hid resp curframe $ k ()
        Remote _ _ :>>= k -> stepReplay k
        Remember _ :>>= k -> stepReplay k
        Nested _   :>>= k -> stepReplay k
        Chain{}  :>>= k -> stepReplay k

      EQ -> case view p of -- Recover Mode
        Remote _ _ :>>= k -> unpackRemote k
        Nested pN  :>>= k -> runNested pN completionHandler cR
          where completionHandler a = Perform (return a) $ \a' -> executeSeksek nextIdx (stateBeforeNesting ++ [toJSON a']) $ k a'
                cR c = makeContinuation curIdx $ stateBeforeNesting ++ [toJSON c]
        Chain h f pN :>>= k -> do
          chainCont'  <- getOneAfter
          chainCont <- maybeFromJSON chainCont' ?< "Internal Error! The JSON value can't be decoded as a chain continuation"
          case chainCont of
            Completed -> unpackRemote k
            Ongoing nestedCont -> do
              nestedResult <- runSeksek resp nestedCont pN
              handleNested curIdx cR cH nestedResult
                where cH a = Ask h (makeContinuation curIdx $ stateBeforeNesting ++ [toJSON Completed]) (f a)
                      cR c = makeContinuation curIdx $ stateBeforeNesting ++ [toJSON $ Ongoing c]
        _ -> throwError $ "The handler(" ++ show curIdx ++ ") of the job is not a Nested, Chain or Remote!"

      GT -> throwError "Internal error, curIdx cannot exceed hid in replaySeksek"

executeSeksek :: Int -> [Value] -> SeksekProgram a -> EitherInstruction a
executeSeksek curIdx state p = let nextIdx = curIdx+1 in
  case view p of
    Return a -> return (Tell a)
    Remote h a :>>= _ -> return (Ask h (makeContinuation curIdx state) a)
    Remember a :>>= k -> return (Perform a $ \out -> executeSeksek nextIdx (state ++ [toJSON out]) $ k out)
    Forget a   :>>= k -> return (Perform a $ \_   -> executeSeksek nextIdx state $ k ())
    Nested pN  :>>= k -> do
      nestedResult <- executeSeksek 0 [] pN
      handleNested curIdx cR completionHandler nestedResult
        where completionHandler a = Perform (return a) $ \a' -> executeSeksek nextIdx (state ++ [toJSON a']) $ k a'
              cR c = makeContinuation curIdx $ state ++ [toJSON c]
    Chain h f pN :>>= _ -> do
      nestedResult <- executeSeksek 0 [] pN
      handleNested curIdx cR cH nestedResult
        where cH a = Ask h (makeContinuation curIdx $ state ++ [toJSON Completed]) (f a)
              cR c = makeContinuation curIdx $ state ++ [toJSON $ Ongoing c]

handleNested :: Int -> (SeksekContinuation [Value] -> SeksekContinuation [Value]) ->
                (a -> SeksekInstruction b) -> SeksekInstruction a -> EitherInstruction b
handleNested curIdx continuationWrapper completionHandler nestedInstruction = let nextIdx = curIdx+1 in
     case nestedInstruction of
       Tell a -> return $ completionHandler a
       Ask h cont i -> return $ Ask h wrappedNested i
         where wrappedNested = continuationWrapper cont
       Perform a k' -> return $ Perform a $ k' >=> handleNested curIdx continuationWrapper completionHandler


conditionProgram :: FromJSON a => (a -> SeksekProgram b) -> SeksekProgram b
conditionProgram prog = getInit >>= prog

performAction :: IO a -> (a -> EitherInstruction b) -> ExceptInstruction IO b
performAction action step = liftIO action >>= (hoistEither . step)

responseOrInitial :: BS.ByteString -> Maybe (Value, SeksekContinuation [Value])
responseOrInitial body = case decodeStrict' body of
  Nothing -> Nothing
  Just value -> case maybeFromJSON value of
    Just (SeksekResponse resp cont) -> Just (resp, cont)
    Nothing -> Just (value, SeksekContinuation 0 [])

showException :: E.SomeException -> IO (Either String a)
showException e = return $ Left $ show e

withConnection :: String -> String -> (BeanstalkServer -> IO a) -> IO a
withConnection host port = E.bracket (connectBeanstalk host port) disconnectBeanstalk

serveSeksek :: FromJSON a => String -> String -> String -> (a -> SeksekProgram ()) -> IO ()
serveSeksek host port appname prog' = let prog = conditionProgram prog'
  in withConnection host port $ \con -> forever $ do
    _ <- watchTube con $ BS.pack appname
    job <- reserveJob con
    result <- E.handle showException $ runExceptT $ do
      (resp, cont) <- responseOrInitial (job_body job) ?? "Couldn't decode the job body"
      instruction <- hoistEither $ runSeksek resp cont prog
      let handle (Ask handler nextCont inp) = liftIO $ call_service con handler appname nextCont inp
          handle (Tell ()) = return ()
          handle (Perform action nextCont) = performAction action nextCont >>= handle
      handle instruction
    case result of
      Right () -> deleteJob con $ job_id job
      Left err -> putStrLn err >> buryJob con (job_id job) 0

mockSeksek' :: SeksekResponse Value [Value] -> SeksekProgram () -> ExceptT String IO ()
mockSeksek' (SeksekResponse resp cont) prog = do
      instruction <- hoistEither $ runSeksek resp cont prog
      let handle (Ask handler nextCont inp) = do
            liftIO $ do
              putStrLn $ "Handler " ++ show (tubeName handler) ++ " is called with input " ++ BSL.unpack (encode inp)
              putStrLn $ "Along with the continuation:" ++ BSL.unpack (encode nextCont)
            mock <- receiveMock
            mockSeksek' (SeksekResponse mock nextCont) prog
          handle (Tell ()) = return ()
          handle (Perform action rest) = performAction action rest >>= handle
      handle instruction

mockSeksek :: FromJSON a => (a -> SeksekProgram ()) -> IO ()
mockSeksek prog = do
  result <- runExceptT $ do
    mock <- receiveMock
    let resp = SeksekResponse mock (SeksekContinuation 0 [])
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
initialJob a = BSL.toStrict $ encode a

sendJob :: ToJSON a => String -> String -> String -> a -> IO (JobState, Int)
sendJob host port appname a = withConnection host port $ \con -> do
  useTube con $ fromString appname
  putJob con 0 0 100 $ initialJob a
