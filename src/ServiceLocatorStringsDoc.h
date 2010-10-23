/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * A file for the ServiceLocatorStrings doxygen page.
 * This file intentionally contains no code.
 */

#ifndef RAMCLOUD_SERVICELOCATORSTRINGSDOC_H
#define RAMCLOUD_SERVICELOCATORSTRINGSDOC_H

namespace RAMCloud {

/**
 * \page ServiceLocatorStrings Service Locator Strings
 *
 * A <em>service locator string</em> specifies how to communicate with a
 * service. It is made up of a prioritized list of alternative ways to
 * communicate with that service, where each includes the protocol to be used
 * and a set of protocol-specific arguments, including addressing information
 * and protocol options.
 *
 * An example of a service locator string looks like this:
 * <pre>fast+udp: host=example.org, port=8081; tcp: host=example.org</pre>
 * This example describes two ways to access the service. The preferred way
 * uses protocols "fast" followed by "udp", having the option "host" set to
 * "example.org" and the option "port" set to "8081". An alternative way (less
 * preferred) to access the service is using the "tcp" protocol, with the
 * option "host" set to "example.org".
 *
 * Each alternative is separated by a ';'. Within each alternative, everything
 * before the ':' is called the \em protocol. Everything after the ':' is
 * called the \em options. Each option is separated by a ',', and within each
 * option, there is a \em key, then an '=', then a \em value. If an option is
 * specified multiple times in the string, only the last occurrence is used.
 *
 * Protocols must be made up of one or more alphanumeric characters,
 * '_', or '+'.
 *
 * Keys must be made up of one or more alphanumeric characters or '_'.
 *
 * Values may be quoted (surrounded by '"', double-quotes) or unquoted. They
 * can be made up of zero or more of any type of character. For quoted values,
 * any enclosed '"' characters should be escaped with '\'. For unquoted values,
 * any enclosed '"', ',', and ';' characters should be escaped with '\'.
 * Leading and trailing spaces are automatically stripped from any unquoted
 * values. There is no support for hexadecimal/octal number specification
 * through a prefix like 0x.
 *
 * Spaces are optional and insignificant before and after each protocol, key,
 * value, ':', '=', ',', and ';'.
 */

} // end RAMCloud

#endif  // RAMCLOUD_SERVICELOCATORSTRINGSDOC_H
