# Memsniff

[![Project Status](http://opensource.box.com/badges/active.svg)](http://opensource.box.com/badges)

Memsniff displays the busiest keys being retrieved from your memcache server.
It has been inspired by the earlier [mctop](https://github.com/etsy/mctop) and
[memkeys](https://github.com/tumblr/memkeys) tools.

Like its predecessors, memsniff listens to network traffic and identifies
responses to `get` requests, and is usually run directly on a memcache server
host.

You can see our announcement
[here](https://blog.box.com/blog/introducing-memsniff-robust-memcache-traffic-analyzer/).


## Installation

Before building you'll need to have the libpcap library and headers installed.

On Redhat-based distributions:

```shell
# yum install libpcap-devel
```

Or on Debian-based distributions:

```shell
# apt-get update && apt-get install libpcap-dev
```

Memsniff uses the
(standard golang toolchain](https://golang.org/doc/install),
which makes installation simple.
Once you have the toolchain installed and `$GOPATH` pointed to your working
directory:

```shell
$ go get github.com/box/memsniff
$ go build github.com/box/memsniff
```

You will find a compiled binary at `$GOPATH/bin/memsniff`,
ready to be transferred to your Memcache
hosts or packaged in your distribution's preferred format.


## Usage

On most operating systems `memsniff` requires superuser privileges to capture
network traffic from an interface, which you specify with the `-i` option.

```shell
# memsniff -i eth0
```

See `-h` for more command-line options.  Once running a few more keys are
active:

* `p` - Pause the updating of the display. Press `p` again to resume.
* `q` - Exit `memsniff`.


## Roadmap

* Support binary memcached protocol
* Support additional operations beyond GET
* Support alternate sorting methods
* Create a stable report format for output to disk
* Automatic logging to disk when specified conditions are met (e.g. aggregate
  or single key traffic exceeds a threshold)
* Break out traffic by client IP
* Supply build support for common package formats (`.deb`, `.rpm`, &hellip;)


## Developing memsniff

Want to contribute? First have a look at
[CONTRIBUTING.md](https://github.com/box/memsniff/blob/master/CONTRIBUTING.md).

#### Running the tests

`memsniff` uses the standard Go testing framework:

```shell
$ go test ./...
?   	github.com/box/memsniff	[no test files]
...
ok  	github.com/box/memsniff/vendor/github.com/spf13/pflag	0.067s
```
3rd party package dependencies are in the `vendor` directory, and you can
significantly speed up test execution time by bypassing this folder.

`memsniff` uses [Glide](https://github.com/Masterminds/glide) to manage its
dependencies, and if you have it installed, you can easily test just
`memsniff`'s packages with Glide's `novendor` command:

```shell
$ go test $(glide nv)
?   	github.com/box/memsniff/analysis	[no test files]
...
ok  	github.com/box/memsniff/protocol	0.009s
?   	github.com/box/memsniff	[no test files])
```

Alternatively, you can do much the same thing with a little `grep` magic:

```shell
$ go test $( go list ./... | grep -v /vendor/ )
?   	github.com/box/memsniff	[no test files]
...
?   	github.com/box/memsniff/presentation	[no test files]
ok  	github.com/box/memsniff/protocol	0.009s
$
```


#### Data pipeline

1. Raw packets are captured on the main thread from `libpcap` using
   [GoPacket](https://www.github.com/google/gopacket).
2. Batches of raw packets are sent to the decode pool, where workers parse the
   memcached protocol looking for responses to `get` requests.  The key and
   size of the value returned are extracted into a response summary.
3. Batches of response summaries are sent to the analysis pool where the stream
   is hash partitioned by cache key and sent to workers. Each worker maintains
   a hotlist of the busiest keys in its hash partition.
4. In response to periodic requests from the UI, the analysis pool merges
   reports from all its workers into a single sorted hotlist, which is
   displayed to the user.


## Support

Need to contact us directly? Email oss@box.com and be sure to include the name
of this project in the subject.


## Copyright and License

Copyright 2017 Box, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
