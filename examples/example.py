from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from experimenter.task import TaskDefinition

main = 'Task1'

##############################################################
task1 = TaskDefinition(
    name='Task1',
    actions=['echo TASK1']
)