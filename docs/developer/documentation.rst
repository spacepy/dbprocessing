*************
Documentation
*************

`Sphinx <http://www.sphinx-doc.org/>`_ documentation sources are stored in the
``docs`` directory; the build system and output are in the ``sphinx`` directory.

``Makefile`` is used to build the documentation.

Run ``make html`` in the ``sphinx`` directory to build the html documentation, output is in ``build/html``.

``sphinx/build`` contains the built documentation (and intermediate files);
it can safely be deleted.

``docs`` contains the ReStructuredText source files; note that a large
quantity of the documentation is not built from here but from the Python
source files. ``docs/developer/autosummary`` contains the docs extracted
from those source files; it can safely be deleted. If anything's weird
about the docs generated from the Python source, try deleting ``autosummary``
first and then rebuilding. The ``autosummary`` docs are extracted from the
version of the module in top-level ``build``, i.e., run
``python setup.py build`` before generating the documentation.

.. contents::
   :local:

Docstring standards
===================
Functions, methods, classes, and (sometimes) data members should be
documented with `Sphinx <http://www.sphinx-doc.org/>`_
docstrings. They must follow `PEP 257
<https://www.python.org/dev/peps/pep-0257/>`_ and `numpydoc
<https://numpydoc.readthedocs.io/en/latest/format.html>`_ standards
(the documentation is built with the numpydoc extension).

Docstrings must describe what a function does and why,
including a full description of the inputs and outputs, including
typing. The docstring must start with a single line brief
description, then a blank line, then any further details (including
any pitfalls, restrictions, or circumstances in which the function is
applicable), ending with the appropriate numpydoc markup for inputs and
outputs. Cross-referencing of documentation should be generous.

If a directive spans multiple lines, proper indentation is essential
for readability and Sphinx parsing. Subsequent lines must align with
the start of the description of the parameter.

Documentation must build in Sphinx without warnings and the output
be checked for proper formatting.

Docstrings should be sufficient for most commenting; if there is
reasonable potential for confusion about *how* a function or a
particular line works, a comment is appropriate. URLs of relevant bug
reports, stackoverflow questions, etc. are particularly useful. TODO
comments are also useful, for extending functionality that may be
needed in the future or checking on corner cases; capitalize them so
they can be found later. Consider opening an issue instead.  Raising a
``NotImplementedError`` may be appropriate in the meantime. Avoid
commenting-out code rather than deleting it, except for short-term
testing; it can always be retrieved from version control later.

Sphinx rst standards
====================
For static Sphinx documentation (i.e. ``.rst`` files), keep in mind the
principles of the docstrings. The `Python documentation style guide
<https://devguide.python.org/documenting/#style-guide>`_ is also a worthy
reference. In particular, note the three-space indentation standard for
documentation (except Python sample code).

All documentation must be in rST (reStructuredText) format except where
another format is explicitly required.

Section headings should follow the :ref:`recommended convention from Sphinx
<sphinx:rst-sections>` with the clarification that most files should be
considered chapters. Parts are very rare and may involve a separate directory,
e.g. "User guide", "Developer Guide."

Most files should have a table of contents for the file near the top.

Wrap lines at the first opportunity past 72 characters, but never exceed
80 characters to a line (which may require an earlier line break.)

Cross-referencing should be generous: to other rST documentation, to API
documentation in docstrings, and to documentation of other projects (where
relevant.) Use :mod:`~sphinx.ext.intersphinx` to link to other projects that
use Sphinx for documentation. ``make linkcheck`` in the ``sphinx`` directory
will verify links (both intersphinx and HTTP).

The documentation is not, at this time, completely up to these standards
(or completely consistent.)

.. _documentation-magic-github:

Magic github documentation
==========================
Several files in the repository are both built into the Sphinx documentation
and used directly by github. For this reason, all documentation source is
in the "docs" directory, which github treats specially. Files that are parsed
by github must still end with the ``.rst`` extension (and github should
render them correctly as rST), but must also be `parseable as Markdown
<https://gist.github.com/dupuy/1855764>`_. ``dbprocessing`` may eventually
:doc:`enable Sphinx parsing of Markdown <sphinx:usage/markdown>` if necessary
to support these files.

See also :doc:`github`.

These documentation files are "magic" to github:
   * `CODE_OF_CONDUCT <https://docs.github.com/en/communities/
     setting-up-your-project-for-healthy-contributions/
     adding-a-code-of-conduct-to-your-project>`_
     (:doc:`../CODE_OF_CONDUCT`)
   * `CONTRIBUTING <https://docs.github.com/en/communities/
     setting-up-your-project-for-healthy-contributions/
     setting-guidelines-for-repository-contributors>`_
     (:doc:`../CONTRIBUTING`)
   * LICENSE
     (:doc:`../LICENSE`)
   * README
     (:doc:`../README`)
   * `SUPPORT <https://docs.github.com/en/communities/
     setting-up-your-project-for-healthy-contributions/
     adding-support-resources-to-your-project>`_
     (:doc:`../SUPPORT`)

The github documentation on `community profiles
<https://docs.github.com/en/communities/
setting-up-your-project-for-healthy-contributions/
about-community-profiles-for-public-repositories>`_ can help determine whether
these files are being parsed properly by github.

More information on possible locations for these files is
in the `github documentation on community health files <https://
docs.github.com/en/communities/
setting-up-your-project-for-healthy-contributions/
creating-a-default-community-health-file>`_.

Release Notes
=============
See :doc:`release`.
