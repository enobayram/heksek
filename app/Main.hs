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
  forget $ do
    putStrLn $ "Received " ++ show out ++ " from test service for " ++ show req ++ "!"
    putStrLn "Let's try another number:"
  another <- remember readLn
  final <- remote test_service (out, another)
  forget $ putStrLn $ "Finally, received " ++ show final

main :: IO ()
main = serveSeksek "localhost" "11300" "heksektest" testProg
