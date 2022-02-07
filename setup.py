from setuptools import setup

setup(
    name='CCSLink',
    version='0.1.0',
    author='Anthony Edwards, Charlie Tomlin, Zoe White',
    author_email='anthony.g.edwards@ons.gov.uk',
    description='A pipeline to match Census data to Census Coverage Survey data. This splits matches into three groups; accepted, rejected and sent to clerical review',
    package_dir={'': '/home/cdsw/collaborative_method_matching/'},
    packages=['CCSLink'],
    install_requires=[
      'pip==21.1.3',
      'pandas==0.24.2',
      'numpy==1.16.2',
      'jellyfish==0.8.2',
      'pyjarowinkler==1.8',
      'nltk==3.6.2',
      'networkx==2.5.1',
      'requests==2.20.0',
      'importlib>=1.0.4',
      'graphframes-wrapper==0.6.0.0',
      'graphframes==0.6'],
    zip_safe=False
)