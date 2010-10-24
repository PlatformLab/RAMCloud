"""
This is a definitions file for the L{pragmas} module.
"""

magic_pattern = 'RAMCloud[\s_-]+pragma.*\[(.*)\]'

gccwarn = PragmaDefinition('GCC warnings', default=9)
gccwarn[0] = 'no warnings'
gccwarn[5] = 'non-fatal warnings'
gccwarn[9] = 'fatal, pedantic warnings'
definitions['GCCWARN'] = gccwarn

cpplint = PragmaDefinition('cpplint.py', default=5)
cpplint[0] = 'disabled'
cpplint[5] = 'standard, fatal checks'
definitions['CPPLINT'] = cpplint
