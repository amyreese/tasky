from setuptools import setup

import tasky

setup(name='tasky',
      description='asyncio framework for task-based execution',
      version=tasky.__version__,
      author='John Reese',
      author_email='john@noswap.com',
      url='https://github.com/jreese/tasky',
      classifiers=['License :: OSI Approved :: MIT License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 3.5',
                   'Topic :: Utilities',
                   'Development Status :: 4 - Beta',
                   ],
      license='MIT License',
      packages=['tasky'],
      )
