{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.Seksek

test_service :: SeksekHandler (Double, Double) Double
test_service = SeksekHandler "test_service"

testProg :: Double -> SeksekProgram ()
testProg inp = do
  forget $ do
    putStrLn $ "Received: " ++ show inp
    putStrLn "Input some number:"
  userInput <- remember readLn
  let req = (inp, userInput)
  forget $ putStrLn $ "Now sending:" ++ show req
  out <- remote test_service req
  nestedOut <- nested $ do
    phony <- remember $ return 150
    nestedRemote <- remote test_service (phony, userInput)
    return $ phony + nestedRemote
  forget $ do
    putStrLn $ "Received " ++ show out ++ " from test service for " ++ show req ++ "!"
    putStrLn "Let's try another number:"
  another <- remember readLn
  final <- remote test_service (out, another)
  forget $ putStrLn $ "Finally, received " ++ show final
  chainOut <- test_service <$< (\x -> (x,x)) <:$> test_service <$< (\x -> (x,x)) <:$> remote test_service (userInput, another)
  forget $ putStrLn $ "Chain output:" ++ show chainOut

main :: IO ()
main = serveSeksek "localhost" "11300" "heksektest" testProg
