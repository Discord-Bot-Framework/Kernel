include PROFILE.local

include globals.local

include allow-python3.inc

blacklist ${RUNUSER}/wayland-*
blacklist ${RUNUSER}
blacklist /usr/libexec
blacklist /boot
blacklist /sys
blacklist /proc/sys
blacklist /proc/irq
blacklist /proc/kallsyms
blacklist /proc/kcore
blacklist /proc/modules
blacklist /proc/sched_debug
blacklist /proc/timer_list
blacklist /dev/kmsg
blacklist /dev/mem
blacklist /dev/port
blacklist /dev/kmem

include disable-common.inc
include disable-devel.inc
include disable-interpreters.inc
include disable-proc.inc
include disable-programs.inc
include disable-shell.inc
include disable-x11.inc
include disable-xdg.inc

caps.drop all

netfilter
protocol unix,inet,inet6

nonewprivs
noroot
nosound
notv
nodvd
nogroups
noinput
nodbus

seccomp
seccomp.block-secondary

disable-mnt
private-cache
private-dev
private-tmp

noexec ${RUNUSER}
noexec /dev/mqueue
noexec /tmp
noexec /var
noexec /dev/shm
noexec /run/shm

restrict-namespaces
