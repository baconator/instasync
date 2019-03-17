# Instasync
A sketch of an idea for hooking [JNR FUSE](https://github.com/SerCeMan/jnr-fuse) up into [P4Java](https://swarm.workshop.perforce.com/projects/perforce-software-p4java) to make something vaguely approximating [VFS for Git](https://github.com/Microsoft/VFSForGit) ... but with Perforce.

Only tested on Windows, so far, and is hardcoded to store its cache on a drive you probably don't have. Requires [WinFSP](https://github.com/billziss-gh/winfsp) installed as a runtime requirement. 