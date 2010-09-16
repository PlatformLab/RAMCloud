{-
Installation:

sudo apt-get install ghc
cabal update
cabal install ansi-terminal

runhaskell Colorize.hs
- or -
ghc --make Colorize.hs -o Colorize

Then just create a script to pipe your junk through Colorize.
make $* 2>&1 | Colorize
exit ${PIPESTATUS[0]}
-}

module Main where

import System.Console.ANSI
import Data.List
import Text.Regex
import System.Posix.Directory
import System.IO.Unsafe

--- Config ---
markup = [ ("error:",           colorPutStrLn Vivid Red)
         , ("!!!FAILURES!!!",   colorPutStrLn Vivid Red)
         , ("undefined",        colorPutStrLn Vivid Red)
         , ("assertion",        colorPutStrLn Vivid Red)
         , ("within",           colorPutStrLn Dull Red)
         , ("note:",            colorPutStrLn Vivid Blue)
         , ("warning:",         colorPutStrLn Vivid Yellow)
         , ("instantiated",     colorPutStrLn Vivid Green)
         , ("from",             colorPutStrLn Vivid Green)
         , ("In",               colorPutStrLn Dull Green)
         , ("g++",              colorPutStrLn Vivid Black)
         , ("ar",               colorPutStrLn Vivid Black)
         , ("perl",             colorPutStrLn Vivid Black)
         , ("mock.py",          colorPutStrLn Vivid Black)
         ]

ss = "std::basic_string<char, std::char_traits<char>, std::allocator<char> >"
substs = [ (ss, "string") ]

prefixsToStrip = [cwd ++ "/src/", cwd ++ "/"]
  where cwd = unsafePerformIO getWorkingDirectory


cleanup = id
        |. elideCompiles ["g++", "ar"]
        |. killFollowing "perl"
        |. stripPaths
        |. applySubsts
        |. markLine

------

-- | Just to make things look like pipes instead of composition.
(|.) = flip (.)
infixl 9 |.

-- | Kill all prefixesToStrip from a line
stripPaths :: String -> String
stripPaths line = foldl stripIfPrefixed line prefixsToStrip

-- | Kill prefix from line.
stripIfPrefixed :: String -> String -> String
stripIfPrefixed line prefix =
  case stripPrefix prefix line of
    Just s -> s
    Nothing -> line

-- | See elideCompile.  Elides compiles for every lines starting with a word
--   in a list.
elideCompiles :: [String] -> String -> String
elideCompiles = flip $ foldl elideCompile

-- | If the first word of line is word then delete all but the first and last
--   word in the line.
elideCompile :: String -> String -> String
elideCompile line word =
  if not (null w) && head w == word
    then unwords [head w, stripPaths $ last w]
    else line
  where w = words line

-- | Replace all words in the line beyond the first with "..." if the first
--   word matches the given word.
killFollowing word line =
  if not (null w) && head w == word
     then unwords [word, "..."]
     else line
  where w = words line

-- | Apply pairs from substs on the line.  The first string in each pair is a
--   regular expression pattern which is replaced with the second string from
--   the pair.  The pairs are applied in order.
applySubsts :: String -> String
applySubsts line = foldl sub line substs
    where sub l (pat, repl) = subRegex (mkRegex pat) l repl

-- | Apply pairs from markup on the line.  For a string from the pair that
--   matches the corresponding action from the pair is performed on the line.
--   Pairs are applied in order; the lastmost match is the action that gets
--   performed.
markLine :: String -> IO ()
markLine l = foldl combine putStrLn markup $ l
    where combine action (word, action') =
            if word `elem` words l
              then action'
              else action

-- | Print line with intensity and color.
colorPutStrLn intensity color line = do
  setSGR [SetColor Foreground intensity color]
  putStrLn line
  setSGR [Reset]

main = do
    contents <- getContents
    sequence_ $ map cleanup $ lines contents

