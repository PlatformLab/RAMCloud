module Main where

import Control.Monad
--import Control.Exception

import RAMCloud as RC

count = 100

main = RC.withClient $ \client -> do
         let write = RC.write client 0
         let read = RC.read client 0

         let vals = take count $ zip (iterate (1+) 0) (repeat "Hello, World, from Haskell!") 

         mapM_ (uncurry write) vals
         putStrLn $ "Wrote " ++ (show count) ++ " values"

         val <- catch (read 1) $ \_ -> return "Read failed"
         val' <- catch (read 1) $ \_ -> return "Read failed"
         -- TODO Still have a problem with multiple reads
         --vals <- mapM read $ take 10 $ iterate (1+) 0
         print val
         print val'
