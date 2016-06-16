{-# LANGUAGE TemplateHaskell, GADTs, RecursiveDo #-}
{-# OPTIONS_GHC -Wall -fno-warn-unused-binds #-}

module Control.Monad.Seksek (
  SeksekHandler(..), SeksekProgram, getInit, remote, remember, serveSeksek,
  initialJob, sendJob, mockSeksek
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
import Data.Typeable
import Control.Monad.Except (throwError, liftIO, runExceptT, ExceptT)
import Control.Monad
import Control.Error.Util ((??), note, hoistEither)
import Control.Monad.Operational (Program, singleton, view, ProgramViewT(..))
import Safe

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

remote :: (ToJSON a, FromJSON b) => SeksekHandler a b -> (a -> SeksekProgram b)
remote h = singleton . Remote h

remember :: (ToJSON a, FromJSON a) => IO a -> SeksekProgram a
remember = singleton . Remember

type SeksekProgram a = Program Seksek a

makeContinuation :: Int -> [Value] -> SeksekContinuation [Value]
makeContinuation hid before = SeksekContinuation hid $ reverse before

safeSplit :: [a] -> Maybe (a, [a])
safeSplit (a:as) = Just (a,as)
safeSplit [] = Nothing

type StateZipper = ([Value], [Value])

data SeksekInstruction a where
  Ask :: (ToJSON inp, FromJSON out) =>
    SeksekHandler inp out -> SeksekContinuation [Value] ->
    inp -> (out -> SeksekProgram a) -> SeksekInstruction a
  Tell :: a -> SeksekInstruction a
  Perform :: IO out -> (out -> Either String (SeksekInstruction a)) -> SeksekInstruction a

(?<) :: Maybe b -> a -> Either a b
(?<) = flip note

type EitherInstruction a = Either String (SeksekInstruction a)
type ExceptInstruction m a = ExceptT String m (SeksekInstruction a)

stepSeksek :: Int -> Int -> StateZipper -> SeksekProgram a -> EitherInstruction a
stepSeksek curIdx hid (before, after) m = if curIdx < hid
  then case view m of
    Return _ -> throwError "No more steps remaining"
    Remote _ _ :>>= k -> stepReplay k
    Remember _ :>>= k -> stepReplay k
  else assertAfterEmpty >>| case view m of
    Return a -> Tell a
    Remote h a :>>= k -> Ask h (makeContinuation (curIdx+1) before) a k
    Remember a :>>= k -> Perform a $ \out -> stepSeksek (curIdx+1) hid (toJSON out:before, after) $ k out
  where
    ma >>| b = ma >> return b
    stepReplay :: FromJSON b => (b -> SeksekProgram a) -> EitherInstruction a
    stepReplay cont = do
      (next, rest) <- safeSplit after ?< "The state array is too short!"
      out <- maybeFromJSON next ?< ("Failed to decode output from seksek response:" ++ show next)
      stepSeksek (curIdx+1) hid (next:before, rest) $ cont out
    assertAfterEmpty = unless (null after) $ throwError "Internal Error! The 'after' list should be empty."

serveSeksek :: FromJSON a => String -> String -> String -> (a -> SeksekProgram ()) -> IO ()
serveSeksek host port appname prog' = let prog = getInit >>= prog' in forever $ do
  con <- connectBeanstalk host port
  forever $ do
    _ <- watchTube con $ BS.pack appname
    job <- reserveJob con
    let resp = decodeStrict' $ job_body job :: Maybe (SeksekResponse Value Value)
    result <- runExceptT $ do
      SeksekResponse rbody (SeksekContinuation hid st) <- resp ?? "Couldn't decode a SeksekResponse from the job body"
      stArr <- maybeFromJSON st ?? "The state needs to be a JSON array"
      instruction <- hoistEither $ stepSeksek 0 hid ([], stArr ++ [rbody] ) prog
      let handle (Ask handler cont inp _) = liftIO $ call_service con handler appname cont inp
          handle (Tell ()) = return ()
          handle (Perform action cont) = performAction action cont >>= handle
      handle instruction
    case result of
      Right () -> deleteJob con $ job_id job
      Left err -> putStrLn err >> buryJob con (job_id job) 0

readErr :: Read a => String -> IO (Either String a)
readErr msg = fmap (note msg . readMay) getLine

performAction :: IO a -> (a -> EitherInstruction b) -> ExceptInstruction IO b
performAction action step = liftIO action >>= (hoistEither . step)

mockStep :: Int -> [Value] -> SeksekProgram () -> ExceptT String IO ()
mockStep stepId state prog = do
      instruction <- hoistEither $ stepSeksek stepId stepId (state,[]) prog
      let handle (Ask handler cont service_input rest) = do
            liftIO $ do
              putStrLn $ "Service " ++ show (tubeName handler) ++ " is called with:"
              BSL.putStrLn $ encode service_input
              putStrLn "Along with the continuation:"
              BSL.putStrLn $ encode cont
              putStrLn "Please enter a response mocking the service"
            mockString <- liftIO BS.getLine
            mockJSON <- decodeStrict' mockString ?? "Unable to decode a JSON value from mock response"
            mockResponse <- maybeFromJSON mockJSON ?? "Unable to convert JSON to the expected type"
            mockStep (handler_id cont) (appstate cont ++ [mockJSON]) $ rest mockResponse
          handle (Tell ()) = return ()
          handle (Perform action cont) = performAction action cont >>= handle
      handle instruction

mockSeksek :: (Typeable a, FromJSON a) => (a -> SeksekProgram ()) -> IO ()
mockSeksek prog' = do
  result <- runExceptT $ do
    rec
      liftIO $ putStrLn $ "Enter a value of type " ++ show (typeOf inp) ++ " as JSON"
      str <- liftIO BS.getLine
      json <- decodeStrict' str ?? "Unable to decode a JSON value from input"
      inp <- maybeFromJSON json ?? "Unable to convert JSON to the expected type"
    mockStep 0 [json] $ prog' inp
  case result of
    Left err -> putStrLn $ "Error! " ++ err
    Right () -> putStrLn "Congrats!"


maybeFromJSON :: FromJSON a => Value -> Maybe a
maybeFromJSON val = case fromJSON val of
  Success s -> Just s
  _ -> Nothing

getInit :: FromJSON a => SeksekProgram a
getInit = remote (SeksekHandler $ fromString "") ()

initialJob :: ToJSON a => a -> BS.ByteString
initialJob a = BSL.toStrict $ encode $ SeksekResponse a $ SeksekContinuation 1 ([] :: [()])

sendJob :: ToJSON a => String -> String -> String -> a -> IO (JobState, Int)
sendJob host port appname a = do
  con <- connectBeanstalk host port
  useTube con $ fromString appname
  putJob con 0 0 100 $ initialJob a
