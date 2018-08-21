from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from experimenter.task import TaskDefinition, load, var

################################################################################

DEP_VAR1=var('DEP_VAR1', 'imported_var1', locals())
DEP_VAR2=var('DEP_VAR2', 'imported_var2', locals())
DEP_VAR3=var('DEP_VAR3', 'var comes from importable.py', locals())

echo_lst = ['echo1', 'echo2']
cmd_lst = ['echo {}'.format(c) for c in echo_lst]

imptask1 = TaskDefinition(
    name='IMPTASK1',
    outputs=['runalways'],
    actions=['echo IMPTASK1'] + cmd_lst,
)

################################################################################
