# Mounts the V-pipe root folder via SSHfs.
#
# The Docker container must run in "privileged" mode for this to work.

if ! test -d /app/vpipe
then
    mkdir /app/vpipe
fi

sshfs \
-o ro \
-oIdentityFile=/app/identities/$VPIPE_IDENTITY \
$VPIPE_USER@$VPIPE_HOST:$VPIPE_ROOT \
/app/vpipe
