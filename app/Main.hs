{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.Seksek

test_service :: SeksekHandler (Double, Double) Double
test_service = SeksekHandler "test_service"

testProg :: Double -> SeksekProgram ()
testProg inp = do
  usr <- remember $ do
    putStrLn $ "Received: " ++ show inp
    putStrLn "Input some number:"
    usr <- readLn
    putStrLn $ "Now sending:" ++ show (inp, usr)
    return usr
  let req = (inp, usr)
  out <- remote test_service req
  remember $ putStrLn $ "Received " ++ show out ++ " from test service for " ++ show req ++ "!"

main :: IO ()
main = serveSeksek "localhost" "11300" "heksektest" testProg
