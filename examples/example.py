from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from experimenter.task import TaskDefinition

main = 'FTask1'

##############################################################
task1 = TaskDefinition(
    name='Task1',
    actions=['echo TASK1'],
    dependencies=['Task2',
                  'task3_param2-300_param3-asdqwe_end',
                  'examples/example.py'
                  ]
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

##############################################################

ftask1 = TaskDefinition(
    name='FTask1',
    params={
            'out': 'FTask1.txt2',
        },
    actions=['cat tmp/{out}'],
    dependencies=['tmp/{out}']
)

ftask2 = TaskDefinition(
    patterns=['tmp/(?P<file>[^ ]+)[.]txt'],
    actions=['touch tmp/{file}.txt', 'echo "asdqwe" > tmp/{file}.txt'],
    dependencies=['tmp/'],
    outputs=['tmp/{file}.txt']
)


ftask3 = TaskDefinition(
    patterns=['(?P<dir>[a-zA-Z/]+)/'],
    actions=['mkdir -p {dir}'],
    outputs=['{dir}/']
)

ftask4 = TaskDefinition(
    patterns=['tmp/(?P<file>[^ ]+).txt2'],
    actions=['cat tmp/{file}.txt > tmp/{file}.txt2', 'cat tmp/{file}.txt >> tmp/{file}.txt2'],
    dependencies=['tmp/{file}.txt'],
    outputs=['tmp/{file}.txt2']
)

##############################################################

ptask2 = TaskDefinition(
    patterns=['echo_(?P<text>[a-zA-Z0-9/]+)'],
    actions=['echo {text}']
)

ptask1 = TaskDefinition(
    name='PTask1',
    dependencies=['echo_{}'.format(i) for i in range(10)] + ['echo_{}'.format(i) for i in range(10)]
)

##############################################################

errortask1 = TaskDefinition(
    name='ERRORTASK1',
    outputs=['tmp/error1.txt', 'tmp/error2.txt'],
    actions=[
        'mkdir -p tmp',
        'touch tmp/error1.txt',
        'sleep 1m',
        'exit 1',
        'touch tmp/error2.txt',
    ]
)
##############################################################

lst0 = ['11', '22', '33']
lst = ['tmp/lst{}-{}-{}-{}.lst'.format(i, j, k, l)
       for i in range(2)
       for j in range(10,15)
       for k in ['asd', 'qwe']
       for l in lst0
       ]
lsttask = TaskDefinition(
    name='LSTTASK',
    patterns = lst,
    dependencies = ['tmp/'],
    outputs=['{MATCH}'],
    actions=[
        'touch {MATCH}'
    ]
)

##############################################################

DEP_VAR1='non_imported_var1'
load('examples/importable.py', locals())
DEP_VAR2='non_imported_var2'

deptask1 = TaskDefinition(
    name='DEPTASK1',
    outputs=['runalways'],
    dependencies=['IMPTASK1'],
    actions=[
        '@ echo {}'.format(DEP_VAR1),
        '@ echo {}'.format(DEP_VAR2),
        '@ echo {}'.format(DEP_VAR3),
    ]
)
