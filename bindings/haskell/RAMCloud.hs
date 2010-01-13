{-# INCLUDE <ramcloud/client.h> #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module RAMCloud (
                 withClient
                , write
                , read
                )
where

import Prelude hiding (read)
import Foreign.C
import Foreign.C.String
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc hiding (free)

data ClientStruct = ClientStruct
type Client = Ptr ClientStruct

type Table = CULong
type Key = CULong

-- pure function
-- "unsafe" means it's slightly faster but can't callback to haskell
foreign import ccall safe "rc_connect" c_rc_connect :: Client -> IO ()
foreign import ccall safe "rc_disconnect" c_rc_disconnect :: Client -> IO ()
foreign import ccall safe "rc_ping" c_rc_ping :: Client -> IO ()
foreign import ccall safe "rc_write" c_rc_write:: Client -> Table -> Key -> CString -> CULong -> CString -> CULong -> IO ()
foreign import ccall safe "rc_read" c_rc_read :: Client -> Table -> Key -> CString -> Ptr CULong -> CString -> Ptr CULong -> IO CInt
foreign import ccall safe "rc_new" c_rc_new:: IO Client
foreign import ccall safe "rc_free" c_rc_free:: Client -> IO ()

new :: IO Client
-- TODO(stutsman) check return for NULL
-- TODO(stutsman) even better, just use malloc or alloca in Haskell
new = c_rc_new

free :: Client -> IO ()
-- TODO(stutsman) check return for NULL
free = c_rc_free

connect :: Client -> IO ()
-- TODO(stutsman) check return and throw an exception if -1
connect = c_rc_connect

-- TODO(stutsman) fix this allocaBytes to get size of from the library
withClient action = allocaBytes 4096 $ \client -> do
  connect client
  action client
  disconnect client

write :: Client -> Table -> Key -> String -> IO ()
-- TODO(stutsman) binary data, exceptions
write client table key dat = withCStringLen dat $ \(cstr, len) ->
                             -- TODO this will be the index data at some point
                             withCStringLen "" $ \(icstr, ilen) -> do
                               c_rc_write client table key cstr (fromIntegral len) icstr (fromIntegral ilen)

read :: Client -> Table -> Key -> IO String
read client table key = withCAStringLen (take max $ repeat ' ') $ \(cstr, cstrlen) ->
                        -- TODO index data
                        withCAStringLen "" $ \(icstr, icstrlen) ->
                        alloca $ \lenp -> do
                        alloca $ \ilenp -> do
                          poke lenp $ fromIntegral cstrlen
                          poke ilenp $ fromIntegral icstrlen
                          r <- c_rc_read client table key cstr lenp icstr ilenp
                          if r == -1
                             then ioError $ userError "RAMCloud: read failed"
                             else do
                               len <- peek lenp
                               peekCAStringLen (cstr, fromIntegral len)
    where max = 4096
                              
disconnect :: Client -> IO ()
-- TODO(stutsman) check return and throw an exception if -1
disconnect = c_rc_disconnect

