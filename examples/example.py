from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from experimenter.task import TaskDefinition

main = 'Task1'

##############################################################
task1 = TaskDefinition(
    name='Task1',
    actions=['echo TASK1'],
    dependencies=['Task2', 'task3_param2-300_param3-asdqwe_end']
)

task2 =  TaskDefinition(
    name='Task2',
    params={
        'param1': 12,
        'param2': '$HOME'
    },
    actions=['echo TASK2 {param1}', 'echo {param2}']
)

task3 =  TaskDefinition(
    patterns=['task3_param2-(?P<param2>[0-9]+)_param3-(?P<param3>[a-z]+)_end'],
    params={
        'param1': 20,
    },
    dependencies=['task4_times-{param2}'],
    actions=['echo TASK3 {param1} {param2} {param3}']
)

task4 = TaskDefinition(
    patterns=['task4_times-(?P<times>[0-9]+)'],
    actions=['echo TASK4 {times}']
)
