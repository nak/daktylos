import setuptools

VERSION = "1.0.0"

setuptools.setup(
    name='daktylos',
    author='John Rusnak',
    author_email='jrusnak@linkedin.com',
    version=VERSION,
    description="low-startup-overhead, scalable, distributed-testing pytest plugin",
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    entry_points={
    },
    classifiers=[
                 "License :: OSI Approved :: BSD License"],
    license='BSD 2-CLAUSE',
    keywords='metrics validation capture',
    url='https://github.com/nak/daktylos',
    download_url="https://github.com/daktylos/dist/%s" % VERSION,
    install_requires=[
        'sqlalchemy',
        'pyyaml'
    ]
)
