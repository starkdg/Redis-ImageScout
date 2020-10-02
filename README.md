# Redis-ImageScout

A Redis module for indexing images.  The module accepts
precomputed perceptual hashes of images and indexes them
for fast efficient retrieval.  A perceptual hash is a
fingerprint robust to small distortions - such as compression
blurr, scaling, etc.  Useful for such things as duplicate
detection and copyright protection of images.  


## Installation

Build and run a docker image:

```
docker build --tag imgscout:0.1 .
docker run --detach --publish 6379:6379 --mount src=imgscoutdata,dst=/data --name imgscout imgscout:0.1
```

The client demo program requires the following dependencies
pre-installed:

```
libpng-dev
libtiff-dev
libjpeg-dev
```

The demo also requires the following but the build will
automatically install it:

```
libphash
libhiredis
```

To build and install the module:

```
cmake .
make
make install
```

To load/unload the module into Redis:

```
module load /usr/local/lib/imgscout.so
module unload imgscout
module list
```

Or put this in the redis.conf configuration:

```
loadmodule /var/local/lib/imgscout.so
```

## Module Commands

The Redis-Imagescout module introduces the mvptree datatype
with the following commands:


```
imgscout.add key hashvalue title [id]
```

adds a new image perceptual hash to the queue for later addition.  When the
new additions reaches a threshold number, the new arrivals are added to the
index in a batch.  To add right away, immediately follow up with the sync
command.  Returns the id integer value assigned to this image.  The title
string is added as a hash field to the key:<id> key.  Optionally, an id integer
can be appended to the end of the command, but this is not the normal use.  


```
imgscout.sync key
```

adds all the recently submitted image perceptual hashes to the index.  Returns
an OK status message.


```
imgscout.query key target-hash radius
```

queries for all perceptual hash targets within a given radius.  Returns an array of results.
Each item in the array is also an array of two items: the title string and the id integer.


```
imgscout.lookup key id
```

looks up an integer id.  Returns the title string.


```
imgscout.size key
```

Returns the number of entries in the index.

```
imgscout.del key id
```

deletes the id from the index. Returns OK status.


## Client Demo Program

Use the `imgscoutclient` utility to add or query the image files
in a given directory.  Run `./imgscoutclient -h` for all the options.
After adding files, be sure to run `./imgscoutclient --cmd sync --key mykey`
to add the recent additions to the index structure.  


