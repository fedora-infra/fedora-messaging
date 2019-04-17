"fedora-messaging consume" now accepts a "--callback-file" argument which will
load a callback function from an arbitrary Python file. Previously, it was
required that the callback be in the Python path.
