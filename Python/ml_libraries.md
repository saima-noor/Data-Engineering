Top 10 ML Frameworks And Libraries

    A Python framework is an interface or tool that allows developers to build ML models easily, without getting into the depth of the underlying algorithms.

    Python libraries are specific files containing pre-written code that can be imported into your code base by using Python’s import feature. This increases your code reusability.

    A Python framework can be a collection of libraries intended to build a model (e.g., machine learning) easily, without having to know the details of the underlying algorithms. An ML developer, however, must at least know how the algorithms work in order to know what results to expect, as well as how to validate them.


#10 Matplotlib
 
    Matplotlib is an interactive, cross-platform library for two-dimensional plotting. It can produce high-quality graphs, charts and plots in several hardcopy formats.

    Advantages:

        Flexible usage: supports both Python and IPython shells, Python scripts, Jupyter Notebook, web application servers and many GUI toolkits (GTK+, Tkinter, Qt, and wxPython).
        Optionally provides a MATLAB-like interface for simple plotting.
        The object-oriented interface gives complete control of axes properties, font properties, line styles, etc.
        Compatible with several graphics backends and operating systems.
        Matplotlib is frequently incorporated in other libraries, such as Pandas.
    
    Disadvantages:

        Because Matplotlib has two different interfaces (object-oriented vs MATLAB-like), a novice developer can become confused.
        Matplotlib is a visualization library, not a data analysis library. For data analysis, you’ll need to combine it with other libraries, like Pandas.


#9 Natural Language Toolkit (NLTK)
    NLTK is a framework and suite of libraries for developing both symbolic and statistical Natural Language Processing (NLP) in Python. It is the standard tool for NLP in Python.

    Advantages:

        The Python library contains graphical examples, as well as sample data.
        Includes a book and cookbook making it easies for beginners to pick up.
        Provides support for different ML operations like classification, parsing, and tokenization functionalities, etc.
        Acts as a platform for prototyping and building research systems.
        Compatible with several languages.

    Disadvantages:

        Understanding the fundamentals of string processing is a prerequisite to using the NLTK framework. Fortunately, the documentation is adequate enough to assist in this pursuit.
        NLTK does sentence tokenization by splitting the text into sentences. This has a negative impact on the performance.
        

#8 Pandas
 
    Pandas is a Python library for providing high-performance, easy-to-use data structures and data analysis tools for the Python programming language.

    Advantages:

        Expressive, fast, and flexible data structures.
        Supports aggregations, concatenations, iteration, re-indexing, and visualizations operations.
        Very flexible usage in conjunction with other Python libraries.
        Intuitive data manipulation using minimal commands.
        Supports a wide range of commercial and academic domains.
        Optimized for performance.

    Disadvantages:

        It is built on matplotlib, meaning a novice programmer has to be familiar with both libraries in order to know which one would be best suited to solve their problem.
        Less suitable for n-dimensional arrays and statistical modeling. Use NumP, SciPy or SciKit Learn instead.
    

#7 Scikit-Learn
 
    The Python library, Scikit-Learn, is built on top of the matplotlib, NumPy, and SciPy libraries. This Python ML library has several tools for data analysis and data mining tasks.

    Advantages:

        Simple, easy to use, and effective.
        In rapid development, and constantly being improved.
        Wide range of algorithms, including clustering, factor analysis, principal component analysis, and more.
        Can extract data from images and text.
        Can be used for NLP.

    Disadvantages:

        This library is especially suited for supervised learning, and not very suited to unsupervised learning applications like Deep Learning.
    

#6 Seaborn
 
    Seaborn is a library for making statistical graphs in Python. It is built on top of matplotlib and also integrated with pandas data structures.

    Advantages:

        Gives more attractive graphs than matplotlib.
        Has built-in plots that matplotlib lacks.
        Uses less code to visualize graphs.
        Smooth integration with Pandas: data visualization and analysis combined!

    Disadvantages:

        Because Seaborn is built on matplotlib, you have to know the latter in order to use the former.
        Seaborn relies on default themes, and as a result is not as customizable as matplotlib.
        

#5 NumPy
 
    NumPy adds multi-dimensional array and matrix processing to Python, as well as a large collection of high-level mathematical functions. It is commonly used for scientific computing and hence, one of the most used Python Packages for machine learning.

    Advantages:

        Intuitive and interactive.
        Offers Fourier transforms, random number capabilities, and other tools for integrating computing languages like C/C++ and Fortran.
        Versatility – other ML libraries like scikit-learn and TensorFlow use NumPy arrays as input; data manipulation packages like Pandas use NumPy under the hood.
        Has terrific open-source community support/contributions.
        Simplifies complex mathematical implementations.

    Disadvantages:

        Can be overkill – do not use when you can get away with Python Lists, instead.
    

#4 Keras
 
    Keras is a very popular ML for Python, providing a high-level neural network API capable of running on top of TensorFlow, CNTK, or Theano.

    Advantages:

        Great for experimentation and quick prototyping.
        Portable.
        Offers easy expression of neural networks.
        Great for use in modeling and visualization.

    Disadvantages:

        Slow, since it needs to create a computational graph before it can perform operations.
    

#3 SciPy
 
    SciPy is a very popular ML library with different modules for optimization, linear algebra, integration and statistics.

    Advantages:
        Great for image manipulation.
        Provides easy handling of mathematical operations.
        Offers efficient numerical routines, including numerical integration and optimization.
        Supports signal processing

    Disadvantages:
        There is both a stack and a library named SciPy. The library is part of the stack. Beginners who don’t know the difference may become confused.
        

#2 Pytorch
 

    PyTorch is a popular ML library for Python based on Torch, which is an ML library implemented in C and wrapped in Lua. It was originally developed by Facebook, but is now used by Twitter, Salesforce, and many other major organizations and businesses.

    Advantages:
        Contains tools and libraries that support Computer Vision, NLP , Deep Learning, and many other ML programs.
        Developers can perform computations on Tensors with GPU acceleration.
        Helps in creating computational graphs.
        Modeling process is simple and transparent.
        The default “define-by-run” mode is more like traditional programming.
        Uses common debugging tools such as pdb, ipdb or PyCharm debugger.
        Uses a lot of pre-trained models and modular parts that are easy to combine.
    
    Disadvantages:
        Because PyTorch is relatively new, there are comparatively fewer online resources to be found. This makes it harder to learn from scratch, although it is intuitive.
        PyTorch is not widely considered to be production-ready compared to Google’s TensorFlow, which is more scalable.
        

#1 TensorFlow

    Originally developed by Google, TensorFlow is an open-source library for high-performance numerical computation using data flow graphs.

    Under the hood, it’s actually a framework for creating and running computations involving tensors. The principal application for TensorFlow is in neural networks, and especially deep learning where it is widely used. That makes it one of the most important Python packages for machine learning

    Advantages:
        Supports reinforcement learning and other algorithms.
        Provides computational graph abstraction.
        Offers a very large community.
        Provides TensorBoard, which is a tool for visualizing ML models directly in the browser.
        Production ready.
        Can be deployed on multiple CPUs and GPUs.

    Disadvantages:
        Runs dramatically slower than other frameworks utilizing CPUs/GPUs.
        Steep learning curve compared to PyTorch.
        Computational graphs can be slow.
        Not commercially supported.
        Not very toolable.