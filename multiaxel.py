#!/usr/bin/env python

##
# MultiAxel is is released under the "Simplified BSD License":
# 
# Copyright 2010 Ryan Williams. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
#    1. Redistributions of source code must retain the above copyright notice, this list of
#       conditions and the following disclaimer.
# 
#    2. Redistributions in binary form must reproduce the above copyright notice, this list
#       of conditions and the following disclaimer in the documentation and/or other materials
#       provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY RYAN WILLIAMS ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# 
# The views and conclusions contained in the software and documentation are those of the
# authors and should not be interpreted as representing official policies, either expressed
# or implied, of Ryan Williams.

##
# Requires axel from http://freshmeat.net/projects/axel/
# 
# This script allows you to download an entire directory of files over FTP
# using multiple connections per file and multiple files at once
# Note: Make sure your FTP host allows many simultatious connections
#
# This script is rather crude, incomplete, undocumented and possibly buggy but
# it was useful to me.


import os, re, sys
from ftplib import FTP
from optparse import OptionParser
from urlparse import urlparse
from subprocess import Popen, PIPE
from time import sleep
from threading import Thread
from getpass import getpass

class Axel(Thread):

    def __init__(self, url, output='./', connections=3, axel=None):
        Thread.__init__(self)
        self.url = url
        self.axel = axel or 'axel'
        self.connections = connections or 3
        self.process = None
        self.finished = False
        self.output = output or './'

        self.completed = 0
        self.speed = 0
        self.state = 'idle'

    def run(self):

        # Make destination path if it's missing
        output_dir = os.path.dirname(self.output)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        cmd = "%s -n %d -o %s %s" % (self.axel, self.connections, self.output, self.url)
        #print "Calling: ", cmd
        self.process = Popen(cmd.split(' '), shell=False, stdout=PIPE)

        while self.process.poll() == None and not self.finished:
            self.update()

        
        self.finished = True
        #print "Done! -- %s" % str(self.process.poll())


    def update(self):
        # Read line
        line = self.process.stdout.readline().strip()

        if self.process.poll() != None and not line:
            self.completed = 100
            self.speed = 0
            self.finished = True

        # Line showing percentage complete
        if line and line[0] == '[':
            # Extract percentage complete
            percent = re.match('^\[([^\]]+)%\]', line)
            if percent:
                self.completed = int(percent.groups(0)[0])

            # Extract current speed
            speed = re.match('.*\[([^\]]+)KB/s\]$', line)
            if speed:
                self.speed = float(speed.groups(0)[0])


class MultiAxel(object):
    def __init__(self, address, user=None, password=None, output=None, axel=None, connections=3, num_files=3):
        o = urlparse(address)

        dest_dir = os.path.basename(o.path.strip('/'))

        self.dir_list_cache = {}

        self.queue = []
        self.threads = []
        self.ftp = FTP()
        self.logged_in = False
        self.finished = False
        self.prev_status = ''

        self.num_files = int(num_files or 3)
        self.scheme = o.scheme
        self.host = o.hostname
        self.base_path = o.path
        self.output = os.path.join((output or './'), dest_dir)
        self.port = o.port or 21
        self.connections = int(connections or 3)
        self.user = user or o.user or os.getenv('USER') or os.getenv('USERNAME')
        self.password = password or o.password
        self.axel = axel or 'axel'

        # Added the initial directory to the queue
        self.add_to_queue(self.base_path)

    def start(self):
        """Start downloading everything"""

        while not self.finished:
            self.update()
            sleep(1)

    def update(self):
        """Run repeatedly until queue is empty"""

        if self.queue and len(self.threads) < self.num_files:
            self.transfer_item()

        total_speed = 0
        for t in self.threads:
            total_speed += t.speed

        self.write_status("Threads: %d/%d -- Queued: %d -- Speed: %.2fKB/s" % (len(self.threads), self.num_files,  len(self.queue), total_speed))

        self.remove_finished_threads()

        if not self.queue and not self.threads:
            self.finished = True

    def remove_finished_threads(self):
        for t in self.threads:
            if t.finished:
                self.write_status("Finished: %s" % t.url)
                self.threads.remove(t)

    def write_status(self, msg):
        print msg

    def login(self):
        if self.logged_in:
            return

        self.logged_in = True
        print "Logging into: %s:%d" % (self.host, self.port) 
        self.ftp.connect(self.host, self.port)
        self.ftp.login(self.user, self.password)
        self.write_status("Logged in")

    def add_to_queue(self, item, index=-1):
        """Add a directory or file to the queue"""
        if not isinstance(item, list):
            item = [item]

        self.write_status("Added to queue:\n  %s" % ('\n  '.join(item)))

        if index == 0: # Prepend to queue
            self.queue = item + self.queue
        elif index == -1 or index == (len(self.queue) -1): # Append to queue
            self.queue += item
        else: # Splice into the middle of queue
            pass # TODO


    def transfer_item(self):
        """Transfer the top item in the queue"""

        if not self.queue: # Queue is empty
            return False

        self.write_status("Transfering item")
        self.download_item(self.queue.pop(0))

        return True

    def download_item(self, item):
        if self.is_file(item):
            self.download_file(item)
        else:
            self.download_directory(item)

    def url_for_path(self, path):
        return "%s://%s:%s@%s:%d%s" % (self.scheme, self.user, self.password, self.host, self.port, path)

    def download_file(self, path):
            
        output_path = path[len(self.base_path):].strip('/')
        output = os.path.join(self.output, output_path)

        self.write_status("Downloading file: %s => %s" % (path, output))

        thread = Axel(self.url_for_path(path), output=output, connections=self.connections)
        self.threads.append(thread)
        thread.start()

    def download_directory(self, path):
        self.write_status("Downloading directory: %s" % path)
        files = [os.path.join(path, f) for f in self.list_directory(path)]
        # Add contents to top of queue
        self.add_to_queue(files, 0)
        
    def list_directory(self, path, force=False):
        self.login()

        # Pull from cached list of directory contents if possible
        if not force and path not in self.dir_list_cache:
            self.dir_list_cache[path] = self.ftp.nlst(path)

        return self.dir_list_cache[path]

    def is_directory(self, path):
        return (not self.is_file(path))

    def is_file(self, path):
        files = self.list_directory(path)
        return (len(files) == 1 and files[0] == path)


def main():
    parser = OptionParser("%prog url [options]")
    parser.add_option("-u", "--user",   dest="user",   help="User to login as. Default is current user")
    parser.add_option("-p", "--pass",   dest="password",   help="Password to login with")
    parser.add_option("-P", "--prompt", dest="prompt", action="store_true", help="Prompt for password")
    parser.add_option("-a", "--axel",   dest="axel",   help="Path to the axel executable if it's not in your PATH")
    parser.add_option("-n", "--num-connections",   dest="connections",   help="Number of connections for axel to use per file. Defauls is 3")
    parser.add_option("-f", "--num-files",   dest="num_files",   help="Number of files to download at once. Default is 3")
    parser.add_option("-o", "--output",   dest="output",   help="Where to save to. Default is current directory")

    (options, args) = parser.parse_args()

    if not args:
        parser.print_help()
        return

    # prompt for password
    if options.prompt:
        password = getpass('Password for "%s": ' % options.user)
    else:
        password = options.password
        
    
    axel = MultiAxel(args[0], user=options.user, password=password, axel=options.axel, output=options.output, connections=options.connections, num_files=options.num_files)
    axel.start()


if __name__ == "__main__":
    main()
