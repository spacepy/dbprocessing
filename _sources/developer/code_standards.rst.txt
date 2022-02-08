**************
Code standards
**************

Code in ``dbprocessing`` should follow consistent standards to maximize
readability.

.. contents::
   :local:

Coding style
============

Several external documents form the basis of the ``dbprocessing`` code style.

    * `PEP 8 <https://www.python.org/dev/peps/pep-0008/>`_.
    * `PEP 20 <https://www.python.org/dev/peps/pep-0020/>`_. Although less
      prescriptive than PEP 8, PEP 20 is still very useful for choosing
      how to write code.
    * `PyHC community standards <https://doi.org/10.5281/zenodo.2529131>`_.

The following specific standards supplement and, where contradictory,
supersede the documents above.

    * Indentation must be with spaces, not tabs. Most editors can
      be configured to insert spaces when the tab key is presssed.
    * Indentation must ordinarily be in increments of four spaces.
    * If the arguments to a function call are split across lines, the
      continuation line should start at the same column that the arguments
      start on the line above (i.e., one column after the opening
      parenthesis of the call.) Multiple-line constants for dictionaries,
      lists, etc. should follow similar formatting. The exception is when
      there are no arguments on the line with the function name and
      opening parenthesis; in that case, all lines with arguments should
      be indented one more level (four spaces) than the beginning of the
      line with the open parenthesis.
    * Lines must break before a binary operator, as suggested in PEP 8.
    * Lines must not be longer than 80 columns unless absolutely necessary.
    * Code must work without change on Python 2.7 and Python 3.
  
      * Where possible, Python 3 should be treated as the "normal" case and
	Python 2 as the exceptional case, e.g. prefer Python 3 names
	from the standard library, try the Python 3 case and fall back
	to Python 2 syntax if it fails, etc. This may not always be
	possible (e.g. if using Python 3 syntax on Python 2 would
	result in successful execution but the wrong result.)

    * ``from import`` and ``import as`` are discouraged, because they
      obscure the origin of the symbols imported, contradicting the
      "explicit is better than implicit" principle of PEP 20. The exception
      is where necessary for maintaining compatibility between Python 2 and 3.

Naming conventions
==================
Variable names should be short and begin with a lower-case letter.

Class names should be CamelCase, with an initial capital letter.

Method names and other identifiers that are multi-word should start
with a lower-case letter, with words separated by ``_``, except for
private identifiers which should start with a single underscore ``_``.

``_`` should be used as the identifier for a "junk" variable.

Updates for style
=================
Much of the ``dbprocessing`` code predates the adoption of these
standards and it will take time to bring it up to standard. Code
should be brought up to standard before being edited for other reasons
(e.g. adding features). Making piecemeal standards-only edits around
the codebase is discouraged; such edits should be comprehensive
(i.e. bring a particular piece of code completely to standard) and
focused (i.e. work on a single function, class, or module at a time,
rather than bits and pieces here and there.)

When updating code for style or standards, it is preferred to perform
all the formatting-only (i.e., non-behavior-modifying) edits first and
place them in one commit before preparing further commits that change
the behavior. There are two reasons for this: first, the formatting
changes will (hopefully) make the code easier to read and understand
before changing it; second, diffs from formatting changes can be
*very* difficult to read and functional changes won't necessarily be
apparent. It is simiarly preferred to verify unit test coverage
before making formatting changes, to ensure that formatting changes do
not introduce regressions.

Style checks
============
``pylint`` can be used to check individual files or the entire package.
There is a ``.pylintrc`` in the top-level checkout which is updated to
``dbprocessing`` standards; if running ``pylint`` from another directory,
specify the path to this file, e.g. from the ``dbprocessing`` directory:

``pylint --rcfile=../.pylintrc inspector.py``

``pylint`` checks will be automated in the future, once existing code is
up to standard.

When updating ``.pylintrc``, leave the default value commented out, so
it is clear where local standards differ from the default. There is no
need to leave previous local values in place. Leave no space between
the comment ``#`` and the default value, to distinguish between defaults
and other comments.

Commit messages
===============
Commit messages should provide a very brief summary of what was done and
why; in particular, placing the changes into context.

They must consist of a single line summary, optionally followed by a
bullet list of details. A blank link must separate the summary and the
details. Each point in the bullet list must start with a single space,
an asterisk, and another space; wrapped lines must be indented three
spaces to align with the start of the line's text.

If a commit closes an issue, include ``(Closes #x)`` at the end of the
first-line summary. See also :doc:`pull_requests`.

Lines must not exceed 76 characters (``git log`` adds a
four-space indent to the commit message on display).

An example:

.. code-block:: none

    Add code of conduct

     * This is marked as rST but in a way that should be compatible with
       Markdown.
     * It's not obvious that github will note this as a "real" CoC but that
       is something that can be dealt with later.


Testing
=======
All code should be tested in the :mod:`unittest`-based test suite
in the ``unit_tests`` directory.

Testing should cover a reasonable fraction of the lines of code: at
least 80%, with 90+% preferred. They should also cover a reasonable
range of possible inputs and input types.

Unit tests using the :meth:`~unittest.TestCase.assertEqual` or similar methods
should specify the expected value as the first argument and the actual output
from the tested method as the second; this makes the displayed diff
rational.  :func:`~numpy.testing.assert_array_equal` and other :mod:`numpy`
based methods should be the other way around (actual, then expected) to make the
diff easy to read.

Checklist
=========
Before finalizing a commit, consider the following questions:

    * Do the unit tests for the recently-edited code complete successfully?
    * Are there sufficient unit tests for the new or recently-edited code?
    * Do unit tests for other code still complete successfully, or is
      it possible they were broken by this change?
    * Does the package install properly?
    * Do the unit tests and installer run properly against Python 2 and 3?
    * Does the documentation build? Does the build raise warnings? Is the
      output correct (e.g. properly formatted)?
    * Is there sufficient documentation? Is there a docstring at all, are
      inputs/outputs described, are cross-references sufficient and properly
      linked?
    * Is the code readable, with appropriate "how" comments, and consistent
      with standards? Are there ways (e.g. particular inputs) the code may
      fail that aren't documented?
    * Is there anything that would obviously make the software hard to
      deploy, such as hard-coded paths?
