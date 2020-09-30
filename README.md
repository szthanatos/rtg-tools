# rtg-tools

English | [中文](README-zh.md)

Of course `rtg` **is not** short for `Radioisotope Thermoelectric Generator`🤣.

**Its not** some kind of `best practice`.  I would like to call it a `standard practice` - on the one hand, it will be a little deeper and closer to actual needs than `basic practice` of official document's "Getting Start Chapter"; on the other hand, if you want to implement it yourself, at least make something better than it.

**It's** a collection about Ready-to-go tools 🔧 for middleware & db in Python.

**It's** a set of code template, with complete comments and user-friendly usage. It allows you to skip most of the details in concepts and syntax, and just access to the middleware/db you want directly.

## Useage

1. find the folder you need.
2. read the readme and comments, examples in the file.
3. clone it, do what ever you want.

## Plan

- rtg-hbase(processing)
- rtg-kafka(tbd)
- rtg-fastdfs(tbd)
- rtg-janusgraph(tbd)
- rtg-rdbs(tbd)
- tbc...

## Contributing

[Contributor Covenant](https://www.contributor-covenant.org/)

Any contributions you make are **greatly appreciated**. Thanks for your fork and commit.

## About

About why this project was written.

3 years ago, I used FastDFS(a file system to store huge amount of small files) in my work. There was no official Python client or document at that time (neither in today). Only a Python2 client was found in community, but our business was going to migrate to Python3.

So we rewrote a Python3 version client. To make it easier to deploy, my colleague uploaded it to [pypi](https://pypi.org/project/py3Fdfs/), and we nearly forget it.

Recently, I find some articles about how to use the client. To be honest, the code is suck, as I didn't spend much time writing it. I hope your boss didn't yell at you for it.

I know what it feels like, you need to use something new, you search the Internet and feel you understand it more or less, but you'r not sure about what you write, does it work effective and correctly. So I decided to summarize my experience, to tell new beginners what to do after reading official documents, or give a "not too bad" implementation for those who only want to accomplish their business.
