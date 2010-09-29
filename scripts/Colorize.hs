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
import Text.Regex.Posix
import System.Posix.Directory
import System.IO.Unsafe
import System.IO

--- Config ---
markup = [ ("error:",           colorPutStrLn BoldIntensity Vivid Cyan)
         , ("!!!FAILURES!!!",   colorPutStrLn NormalIntensity Vivid Cyan)
         , ("undefined",        colorPutStrLn NormalIntensity Vivid Cyan)
         , ("assertion",        colorPutStrLn NormalIntensity Vivid Cyan)
         , ("within",           colorPutStrLn NormalIntensity Dull Cyan)
         , ("note:",            colorPutStrLn NormalIntensity Vivid White)
         , ("warning:",         colorPutStrLn NormalIntensity Vivid Yellow)
         , ("instantiated",     colorPutStrLn NormalIntensity Vivid Green)
         , ("from",             colorPutStrLn NormalIntensity Vivid Green)
         , ("In",               colorPutStrLn NormalIntensity Dull Green)
         , ("g++",              colorPutStrLn NormalIntensity Dull Yellow)
         , ("ar",               colorPutStrLn NormalIntensity Dull Yellow)
         , ("perl",             colorPutStrLn NormalIntensity Dull Yellow)
         , ("mock.py",          colorPutStrLn NormalIntensity Dull Yellow)
         , ("cpplint",          colorPutStrLn NormalIntensity Dull Yellow)
         , ("make:",            colorPutStrLn BoldIntensity Dull Green)
         ]

ss = "std::basic_string<char, std::char_traits<char>, std::allocator<char> >"
substs = [ (ss, "string")
         , ("python cpplint.py", "cpplint")
         ]

prefixsToStrip = [cwd ++ "/src/", cwd ++ "/"]
  where cwd = unsafePerformIO getWorkingDirectory


cleanup = id
        |. elideCompiles ["g++", "ar"]
        |. killFollowing "perl"
        |. killFollowing "python cpplint.py"
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
  if line =~ ("^" ++ word)
     then unwords [word, "..."]
     else line

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

-- | Print line with consoleIntensity, colorIntensity and color.
colorPutStrLn consoleIntensity colorIntensity color line = do
  setSGR [SetConsoleIntensity consoleIntensity,
          SetColor Foreground colorIntensity color]
  putStrLn line
  setSGR [Reset]
  hFlush stdout

main = do
    contents <- getContents
    sequence_ $ map cleanup $ lines contents

