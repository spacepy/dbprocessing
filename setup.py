#!/usr/bin/env python
"""Setup script for dbprocessing"""

use_setuptools = False
try:
    import setuptools
    import setuptools.command.sdist
    use_setuptools = True
except ImportError:
    import distutils.core
    import distutils.command.sdist
import glob
import os
import os.path
import subprocess


class sdist((setuptools.command.sdist if use_setuptools
            else distutils.command.sdist).sdist):
    """Rebuild docs before making a source distribution"""

    def run(self):
        self.run_command('build')  # So sphinx finds latest docstrings
        thisdir = os.path.abspath(os.path.dirname(__file__))
        builddir = os.path.join(os.path.join(thisdir, 'sphinx', 'build',
                                             'doctrees'))
        indir = os.path.join(thisdir, 'docs')
        outdir = os.path.join(thisdir, 'sphinx', 'build', 'html')
        cmd = [os.environ.get('SPHINXBUILD', 'sphinx-build'),
               '-b', 'html', '-d', builddir, indir, outdir]
        subprocess.check_call(cmd)
        (setuptools.command.sdist if use_setuptools
         else distutils.command.sdist).sdist.run(self)

scripts = glob.glob(os.path.join('scripts', '*.py'))

setup_kwargs = {
    'author': 'dbprocessing team',
    'author_email': 'Jonathan.Niehof@unh.edu',
    'classifiers': ['Development Status :: 4 - Beta',
                    'Intended Audience :: Science/Research',
                    'License :: OSI Approved :: BSD License',
                    'Operating System :: POSIX',
                    'Operating System :: POSIX :: Linux',
                    'Programming Language :: Python',
                    'Programming Language :: Python :: 3',
                    'Topic :: Scientific/Engineering :: Astronomy',
                    'Topic :: Scientific/Engineering :: Atmospheric Science',
                    'Topic :: Scientific/Engineering :: Physics'],
    'cmdclass': {'sdist': sdist},
    'description': 'database-driven Heliophysics processing controller',
    'keywords': ['Heliophysics', 'data.processing'],
    'license': 'BSD',
    'long_description': 'database-driven Heliophysics processing controller',
    'maintainer': 'Jonathan Niehof, Denis Nadeau',
    'maintainer_email': 'Jonathan.Niehof@unh.edu',
    'name': 'dbprocessing',
    'packages': ['dbprocessing'],
    'platforms': ['Linux', 'Unix'],
    'provides': ['dbprocessing'],
    'requires': ['python (>=2.7, !=3.0)', 'python_dateutil', 'sqlalchemy'],
    'scripts': scripts,
    'url': 'https://spacepy.github.io/dbprocessing/',
    'version': '0.1.0',
}

if use_setuptools:
    setup_kwargs['install_requires'] = [
        'python_dateutil',
        'sqlalchemy',
    ]

(setuptools if use_setuptools else distutils.core).setup(**setup_kwargs)
