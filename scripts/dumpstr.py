# Copyright (c) 2011 Stanford University
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

"""
A library with which to upload reports to the dumpstr tool.
"""

from __future__ import division, print_function

import json
import os
import sys
import urllib

class Dumpstr(object):
    """
    A class for working with reports.
    """

    class UploadException(Exception):
        def __init__(self, code, response):
            self.code = code
            self.response = response
        def __str__(self):
            return 'Code: %d\n%s' % (self.code, self.response)

    def __init__(self, url):
        """Constructor.

        url -- the base URL for the root of your  dumpstr installation
        """

        if url.endswith('/'):
            url = url[:-1]
        self.url = url

    @staticmethod
    def print_report(data, file=sys.stdout):
        """Print a report to the console.

        The output is not super pretty; if you need that, use the web
        interface.
        """

        out = []
        for section in data:
            out.append('=== {0:} ==='.format(section['key']))
            for line in section['lines']:
                if type(line['summary']) is list:
                    summary = ' / '.join(line['summary'])
                elif type(line['summary']) is float:
                    summary = '{0:7.2f}' % line['summary']
                else:
                    summary = str(line['summary'])
                out.append('{0:<45} {1:}'.format(
                    '{0:}:'.format(line['key']), summary))
            out.append('')
        print('\n'.join(out), file=file)

    def upload_report(self, type, data=None, trends=None, owner=None):
        """Upload (submit) a report.

        type -- a string identifying the type of report
        data -- the main report in a JSON-able format as described in the
                dumpstr README
        trends -- the trends list in a JSON-able format as described in the
                  dumpstr README
        owner -- your username, defaults to the environment variable USER
        """

        if owner is None:
            owner = os.getenv('USER')
        if not data and not trends:
            raise RuntimeError('The report to upload is empty')
        if data is None:
            json_data = ''
        else:
            json_data = json.dumps(data)
        if trends is None:
            json_trends = '[]'
        else:
            json_trends = json.dumps(trends)
        post_data = urllib.urlencode({
                'type': type,
                'owner': owner,
                'data': json_data,
                'trends': json_trends,
            })
        try:
            response = urllib.urlopen(self.url + '/ajax/report/new', post_data)
        except IOError as e:
            raise self.UploadException(0, str(e))
        code = response.getcode()
        if code != 200:
            try:
                info = response.read()
            except e:
                info = e
            raise self.UploadException(code, info)
        report_id = response.read()
        return {'id': report_id,
                'url': self.url + '/report/' + report_id}

if __name__ == '__main__':
    print("This isn't a command line utility, it's a library!")
    sys.exit(1)
