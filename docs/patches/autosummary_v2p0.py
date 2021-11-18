"""Patches to extend autosummary for Sphinx 2.0 (and 1.8)

Sphinx autosummary doesn't get all attributes, and it doesn't include
data members. These patches address those limitations.
"""

# Monkey-patch to put data items in automodule
# This STARTS verbatim from 2.0.1, then new code is commented with ##
# From sphinx/ext/autosummary/generate.py,
# Copyright 2007-2019 by the Sphinx team, BSD license
# Also works w/o change on 1.8 (only difference is type hints)
import os.path  ## NEW
from jinja2 import FileSystemLoader, TemplateNotFound
from jinja2.sandbox import SandboxedEnvironment

from sphinx import package_dir
import sphinx.errors  ## NEW
from sphinx.ext.autosummary import import_by_name, get_documenter
from sphinx.jinja2glue import BuiltinTemplateLoader
from sphinx.pycode import ModuleAnalyzer  ## NEW
from sphinx.locale import __
from sphinx.util.inspect import safe_getattr
from sphinx.util.osutil import ensuredir
from sphinx.util.rst import escape as rst_escape
## NEW: these imports bring in definitions that are available in original
from sphinx.ext.autosummary.generate import _simple_warn, _simple_info, \
    find_autosummary_in_files, _underline


def generate_autosummary_docs(sources, output_dir=None, suffix='.rst',
                              warn=_simple_warn, info=_simple_info,
                              base_path=None, builder=None, template_dir=None,
                              imported_members=False, app=None):
    # type: (List[str], str, str, Callable, Callable, str, Builder, str, bool, Any) -> None

    showed_sources = list(sorted(sources))
    if len(showed_sources) > 20:
        showed_sources = showed_sources[:10] + ['...'] + showed_sources[-10:]
    info(__('[autosummary] generating autosummary for: %s') %
         ', '.join(showed_sources))

    if output_dir:
        info(__('[autosummary] writing to %s') % output_dir)

    if base_path is not None:
        sources = [os.path.join(base_path, filename) for filename in sources]

    # create our own templating environment
    template_dirs = None  # type: List[str]
    template_dirs = [os.path.join(package_dir, 'ext',
                                  'autosummary', 'templates')]

    template_loader = None  # type: Union[BuiltinTemplateLoader, FileSystemLoader]
    if builder is not None:
        # allow the user to override the templates
        template_loader = BuiltinTemplateLoader()
        template_loader.init(builder, dirs=template_dirs)
    else:
        if template_dir:
            template_dirs.insert(0, template_dir)
        template_loader = FileSystemLoader(template_dirs)
    template_env = SandboxedEnvironment(loader=template_loader)
    template_env.filters['underline'] = _underline

    # replace the builtin html filters
    template_env.filters['escape'] = rst_escape
    template_env.filters['e'] = rst_escape

    # read
    items = find_autosummary_in_files(sources)

    # keep track of new files
    new_files = []

    # write
    for name, path, template_name in sorted(set(items), key=str):
        if path is None:
            # The corresponding autosummary:: directive did not have
            # a :toctree: option
            continue

        path = output_dir or os.path.abspath(path)
        ensuredir(path)

        try:
            name, obj, parent, mod_name = import_by_name(name)
        except ImportError as e:
            warn('[autosummary] failed to import %r: %s' % (name, e))
            continue

        fn = os.path.join(path, name + suffix)

        # skip it if it exists
        if os.path.isfile(fn):
            continue

        new_files.append(fn)

        with open(fn, 'w') as f:
            doc = get_documenter(app, obj, parent)

            if template_name is not None:
                template = template_env.get_template(template_name)
            else:
                try:
                    template = template_env.get_template('autosummary/%s.rst'
                                                         % doc.objtype)
                except TemplateNotFound:
                    template = template_env.get_template('autosummary/base.rst')

            def get_members(obj, typ, include_public=[], imported=True):
                # type: (Any, str, List[str], bool) -> Tuple[List[str], List[str]]
                items = []  # type: List[str]
                for name in dir(obj):
                    try:
                        value = safe_getattr(obj, name)
                    except AttributeError:
                        continue
                    documenter = get_documenter(app, value, obj)
                    if documenter.objtype == typ:
                        if imported or getattr(value, '__module__', None) == obj.__name__:
                            # skip imported members if expected
                            items.append(name)
                public = [x for x in items
                          if x in include_public or not x.startswith('_')]
                return public, items

            ns = {}  # type: Dict[str, Any]

            if doc.objtype == 'module':
                ns['members'] = dir(obj)
                ns['functions'], ns['all_functions'] = \
                    get_members(obj, 'function', imported=imported_members)
                ns['classes'], ns['all_classes'] = \
                    get_members(obj, 'class', imported=imported_members)
                ns['exceptions'], ns['all_exceptions'] = \
                    get_members(obj, 'exception', imported=imported_members)
                ## NEW
                ns['data'], ns['all_data'] = \
                    get_members(obj, 'data', imported=imported_members)
                ns['attributes'], ns['all_attributes'] = \
                    get_members(obj, 'attribute', imported=imported_members)
                # Get module-level data attributes
                realmodule = getattr(obj, '__name__', None)
                if realmodule:
                    #Keyed by a tuple of (class name, attribute name)
                    #Result is a list of docstrings
                    try:
                        docattrs = ModuleAnalyzer.for_module(
                            realmodule).find_attr_docs()
                    except sphinx.errors.PycodeError:
                        pass  # No source code (e.g. internal module)
                    else:
                        moreattrs = [k[1] for k in docattrs.keys()
                                     if k[1] not in ns['all_attributes']
                                     and k[0] == '']  # module-level only
                        ns['all_attributes'].extend(moreattrs)
                        ns['attributes'].extend(
                            [a for a in moreattrs if not a.startswith('_')])
                ## END NEW
            elif doc.objtype == 'class':
                ns['members'] = dir(obj)
                ns['inherited_members'] = \
                    set(dir(obj)) - set(obj.__dict__.keys())
                ns['methods'], ns['all_methods'] = \
                    get_members(obj, 'method', ['__init__'])
                ns['attributes'], ns['all_attributes'] = \
                    get_members(obj, 'attribute')
                ## NEW
                # Try to get stuff that's only in class attributes
                if hasattr(obj, '__module__'):
                    realmodule = obj.__module__
                elif hasattr(parent, '__module__'):
                    realmodule = parent.__module__
                else:
                    realmodule = None
                if realmodule:
                    #Keyed by a tuple of (class name, attribute name)
                    #Result is a list of docstrings
                    try:
                        docattrs = ModuleAnalyzer.for_module(
                            realmodule).find_attr_docs()
                    except sphinx.errors.PycodeError:
                        pass  # No source code (e.g. internal module)
                    else:
                        moreattrs = [k[1] for k in docattrs.keys()
                                     if k[1] not in ns['all_attributes']
                                     and k[0] == obj.__name__]
                        ns['all_attributes'].extend(moreattrs)
                        ns['attributes'].extend(
                            [a for a in moreattrs if not a.startswith('_')])
                ## END NEW

            parts = name.split('.')
            if doc.objtype in ('method', 'attribute'):
                mod_name = '.'.join(parts[:-2])
                cls_name = parts[-2]
                obj_name = '.'.join(parts[-2:])
                ns['class'] = cls_name
            else:
                mod_name, obj_name = '.'.join(parts[:-1]), parts[-1]

            ns['fullname'] = name
            ns['module'] = mod_name
            ns['objname'] = obj_name
            ns['name'] = parts[-1]

            ns['objtype'] = doc.objtype
            ns['underline'] = len(name) * '='

            rendered = template.render(**ns)
            f.write(rendered)

    # descend recursively to new files
    if new_files:
        generate_autosummary_docs(new_files, output_dir=output_dir,
                                  suffix=suffix, warn=warn, info=info,
                                  base_path=base_path, builder=builder,
                                  template_dir=template_dir, app=app)
