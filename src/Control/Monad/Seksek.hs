{-# LANGUAGE TemplateHaskell, GADTs #-}
{-# OPTIONS_GHC -Wall -fno-warn-unused-binds #-}

module Control.Monad.Seksek (
  SeksekHandler(..), SeksekProgram, getInit, remote, remember, serveSeksek,
  initialJob, sendJob,
  ) where

import Network.Beanstalk (BeanstalkServer, connectBeanstalk, useTube, watchTube,
                          putJob, reserveJob, deleteJob, buryJob, job_body,
                          job_id, JobState)
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.ByteString.Char8 as BS
import Data.Aeson (defaultOptions, Value(Array), ToJSON, FromJSON, encode,
                   toJSON, fromJSON, decodeStrict', Result(Success))
import Data.Aeson.TH (deriveJSON)
import Data.String (fromString)
import qualified Data.Vector as V
import Control.Monad.Except (throwError, liftIO, runExceptT)
import Control.Monad
import Control.Error.Util ((??), note, hoistEither)
import Control.Monad.Operational (Program, singleton, view, ProgramViewT(..))

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

call_service :: ToJSON inp => BeanstalkServer -> SeksekHandler inp out -> String -> SeksekContinuation Value -> inp -> IO ()
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

makeContinuation :: Int -> [Value] -> SeksekContinuation Value
makeContinuation hid before = SeksekContinuation hid $ Array $ V.reverse $ V.fromList before

safeSplit :: [a] -> Maybe (a, [a])
safeSplit (a:as) = Just (a,as)
safeSplit [] = Nothing

type StateZipper = ([Value], [Value])

data SeksekInstruction a where
  Ask :: (ToJSON inp, FromJSON out) =>
    SeksekHandler inp out -> SeksekContinuation Value ->
    inp -> (out -> SeksekProgram a) -> SeksekInstruction a
  Tell :: a -> SeksekInstruction a
  Perform :: IO out -> (out -> Either String (SeksekInstruction a)) -> SeksekInstruction a

(?<) :: Maybe b -> a -> Either a b
(?<) = flip note

stepSeksek :: Int -> Int -> StateZipper -> SeksekProgram a -> Either String (SeksekInstruction a)
stepSeksek curIdx hid (before, after) m =
  if curIdx > hid then throwError "Internal Error!" else case view m of
    Return a -> if curIdx == hid then return $ Tell a else throwError "No more steps remaining"
    Remote h a :>>= k -> if curIdx < hid
      then do
        (next, rest) <- safeSplit after ?< "The state array is too short!"
        out <- maybeFromJSON next ?< ("Failed to decode output from seksek response:" ++ show next)
        stepSeksek (curIdx+1) hid (next:before, rest) $ k out
      else case after of -- curIdx == hid
        [] -> return $ Ask h (makeContinuation (hid+1) before) a k
        _  -> throwError "Internal Error! The 'after' list should be empty."
    Remember a :>>= k -> if curIdx < hid
      then do
        (next, rest) <- safeSplit after ?< "The state array is too short!"
        out <- maybeFromJSON next ?< ("Failed to decode previously remembered value:" ++ show next)
        stepSeksek curIdx hid (next:before, rest) $ k out
      else case after of
        [] -> return $ Perform a $ \out -> stepSeksek curIdx hid (toJSON out:before, after) $ k out
        _ -> throwError "Internal Error! The 'after' list should be empty."

serveSeksek :: String -> String -> String -> SeksekProgram () -> IO ()
serveSeksek host port appname prog = forever $ do
  con <- connectBeanstalk host port
  forever $ do
    _ <- watchTube con $ BS.pack appname
    job <- reserveJob con
    let resp = decodeStrict' $ job_body job :: Maybe (SeksekResponse Value Value)
    result <- runExceptT $ do
      SeksekResponse rbody (SeksekContinuation hid st) <- resp ?? "Couldn't decode a SeksekResponse from the job body"
      stArr <- maybeFromJSON st ?? "The state needs to be a JSON array"
      instruction <- hoistEither $ stepSeksek 0 hid ([], V.toList stArr ++ [rbody] ) prog
      let handle (Ask handler cont inp _) = liftIO $ call_service con handler appname cont inp
          handle (Tell ()) = return ()
          handle (Perform action cont) = do
            out <- liftIO action
            next <- hoistEither $ cont out
            handle next
      handle instruction
    case result of
      Right () -> deleteJob con $ job_id job
      Left err -> putStrLn err >> buryJob con (job_id job) 0

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
