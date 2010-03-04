"""
This is a definitions file for the L{pragmas} module.
"""

magic_pattern = 'RAMCloud[\s_-]+pragma.*\[(.*)\]'

gccwarn = PragmaDefinition('GCC warnings', default=9)
gccwarn[5] = 'non-fatal warnings'
gccwarn[9] = 'fatal, pedantic warnings'
definitions['GCCWARN'] = gccwarn

cpplint = PragmaDefinition('cpplint.py', default=5)
cpplint[0] = 'disabled'
cpplint[5] = 'standard, fatal checks'
definitions['CPPLINT'] = cpplint

doxygen = PragmaDefinition('Doxygen', default=5)
doxygen[5] = 'standard, fatal checks'
doxygen[9] = 'anal, fatal checks'
definitions['DOXYGEN'] = doxygen
