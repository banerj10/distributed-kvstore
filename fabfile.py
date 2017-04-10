from fabric.api import *

s = 'sp17-cs425-g15-{:02d}.cs.illinois.edu'

env.hosts = [s.format(x) for x in range(1, 11)]
env.user = 'sgupta80'

def deploy():
    with cd('~/mp2'):
        run('git pull')

def git_clone():
    with cd('~'):
        run('git clone git@gitlab.engr.illinois.edu:sgupta80/ece428-mp2.git mp2')

def install_py36():
    run('sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm')
    run('sudo yum -y install python36u python36u-pip')
