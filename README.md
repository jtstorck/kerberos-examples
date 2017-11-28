# kerberos-examples
Usage examples of Kerberos.

## [ugi-test](https://github.com/jtstorck/ugi-test)
The ugi-test project was moved to a separate repository, [ugi-test](https://github.com/jtstorck/ugi-test), and added as a submodule to this repository.  Depending on how much I like working with submodules, it might be kept this way, converted to a subtree, or reverted back to a subdirectory.  For now, I'll keep the submodule in this repository's master branch to the latest commit in [ugi-test](https://github.com/jtstorck/ugi-test).

To clone the repository, run ``git clone https://github.com/jtstorck/kerberos-examples --recursive`` to retrieve the submodule during the clone.

If you've cloned already without ``--recursive``, you’ll need to make sure hadoop/ugi-test is an empty directory.  Move any IDE/build related files/directories temporarily out of hadoop/ugi-test, and then run ``git submodules update --init --recursive``.  Once that's complete, [ugi-test](https://github.com/jtstorck/ugi-test) will be cloned into the hadoop/ugi-test directory.  Now, you can copy any artifacts that were moved out before the submodule update back into hadoop/ugi-test.
