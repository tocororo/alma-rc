rsync -avzP --no-perms --no-owner --no-group --delete --exclude='.venv' --exclude='.git' --exclude='.data' -e 'ssh' ./ root@10.2.6.168:/alma/rc
