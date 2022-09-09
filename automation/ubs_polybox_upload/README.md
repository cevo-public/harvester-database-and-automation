About
-----

Uploads V-pipe results for samples submitted by Universitätsspital Basel from Euler to Polybox.

How to setup
------------

- settings.py includes locations of polybox folders, account info etc

- save public key to access euler and adapt settings.py to  the name/location of this key file

How to run
----------

The password to access polybox must be specified as an environment variable:

```
$ PASSWORD=<PASSWORD> python sync_euler_polybox.py
```

Internals
---------
Uses WebDAV API of polybox.ethz.ch, documented at
https://docs.nextcloud.com/server/latest/user_manual/en/files/access_webdav.html#accessing-files-using-curl

