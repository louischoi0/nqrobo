from setuptools import setup, find_packages
  
#https://code.tutsplus.com/ko/tutorials/how-to-write-your-own-python-packages--cms-26076

setup(name='nqrobo',
      version='1.0',
      url='',
      license='QRAFT',
      author='QRAFT',
      author_email='',
      description='',
      packages=find_packages(),
      zip_safe=False,
      setup_requires=[],
      install_requires=[
          'pandas',
          'numpy',
          'scipy',
          'sqlalchemy',
          'paramiko',
          'stringcase',
          'python-telegram-bot'
          ])

