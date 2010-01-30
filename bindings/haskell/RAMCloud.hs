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
import Foreign.Marshal.Alloc

data ClientStruct = ClientStruct
type Client = Ptr ClientStruct

type Table = CULong
type Key = CULong

-- pure function
-- "unsafe" means it's slightly faster but can't callback to haskell
foreign import ccall safe "rc_connect" c_rc_connect :: Client -> IO ()
foreign import ccall safe "rc_disconnect" c_rc_disconnect :: Client -> IO ()
foreign import ccall safe "rc_ping" c_rc_ping :: Client -> IO ()
foreign import ccall safe "rc_write" c_rc_write:: Client -> Table -> Key -> CString -> CULong -> IO ()
foreign import ccall safe "rc_read" c_rc_read :: Client -> Table -> Key -> CString -> Ptr CULong -> IO CInt

connect :: Client -> IO ()
-- TODO(stutsman) check return and throw an exception if -1
connect = c_rc_connect

disconnect :: Client -> IO ()
-- TODO(stutsman) check return and throw an exception if -1
disconnect = c_rc_disconnect

-- TODO(stutsman) fix this allocaBytes to get size of from the library
-- TODO(stutsman) use Reader on client?  Own monad with read/write defined?
withClient action = allocaBytes 4096 $ \client -> do
  connect client
  action client
  disconnect client

write :: Client -> Table -> Key -> String -> IO ()
-- TODO(stutsman) binary data, exceptions
write client table key dat = withCStringLen dat $ \(cstr, len) -> do
                               c_rc_write client table key cstr (fromIntegral len)

read :: Client -> Table -> Key -> IO String
read client table key = withCAStringLen (take max $ repeat ' ') $ \(cstr, cstrlen) ->
                        alloca $ \lenp -> do
                          poke lenp $ fromIntegral cstrlen
                          r <- c_rc_read client table key cstr lenp
                          if r == -1
                             then ioError $ userError "RAMCloud: read failed"
                             else do
                               len <- peek lenp
                               peekCAStringLen (cstr, fromIntegral len)
    where max = 4096
                              

