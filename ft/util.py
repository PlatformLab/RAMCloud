# Copyright (c) 2010 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import time

def gettime():
    # returns nanoseconds with microsecond precision
    return int(time.time() * 1000 * 1000 * 1000)


class Buffer(list):
    def getTotalLength(self):
        return sum(map(len, self))

    def getRange(self, start, length):
        return ''.join(self)[start:(start + length)]

    def prepend(self, s):
        self.insert(0, s)

    def allocate(self, x):
        return x

class BitVector(object):
    def __init__(self, length, ones=False, seq=None):
        self.length = length
        if seq is not None:
            assert len(seq) == (length + 7) / 8, (length, seq)
            if type(seq[0]) == str:
                self.byteArray = map(ord, seq)
            else:
                self.byteArray = seq[:]
        else:
            if ones:
                byte = 0xFF
            else:
                byte = 0x00
            self.byteArray = [byte] * ((length + 7) / 8)

    def __len__(self):
        return self.length

    def ffz(self):
        for bit, value in enumerate(self.iterBits()):
            if not value:
                return bit
        return None

    def ffs(self):
        for bit, value in enumerate(self.iterBits()):
            if value:
                return bit
        return None

    def iterBits(self):
        for bit in range(self.length):
            yield self.getBit(bit)

    def getBit(self, bit):
        return bool(self.byteArray[bit / 8] & (1 << (bit % 8)))

    def setBit(self, bit):
        self.byteArray[bit / 8] |= (1 << (bit % 8))

    def clearBit(self, bit):
        self.byteArray[bit / 8] &= ~(1 << (bit % 8))

    def __str__(self):
        return ''.join(map(chr, self.byteArray))

    def fillBuffer(self, bufferToFill):
        bufferToFill.append(str(self))

class Ring(object):
    def __init__(self, length, clearValue):
        self._clearValue = clearValue
        self._array = [clearValue] * length
        self._start = 0

    def __getitem__(self, index):
        if index >= len(self._array):
            raise IndexError
        index += self._start
        if index >= len(self._array):
            index -= len(self._array)
        return self._array[index]

    def __setitem__(self, index, value):
        if index >= len(self._array):
            raise IndexError
        index += self._start
        if index >= len(self._array):
            index -= len(self._array)
        self._array[index] = value

    def __len__(self):
        return len(self._array)

    def clear(self):
        for i in range(len(self._array)):
            self._array[i] = self._clearValue

    def advance(self, distance):
        x = list(self)[distance:] + [self._clearValue] * distance
        # TODO(ongaro): assert distance <= len(self._array)?
        for i in range(min(distance,
                           len(self._array))):
            self[i] = self._clearValue
        self._start += distance
        while self._start >= len(self._array):
            self._start -= len(self._array)
        y = list(self)
        assert x == y, (x, y, self.__dict__)
