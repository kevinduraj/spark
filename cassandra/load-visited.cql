-- ------------------------------------------------------ --
-- remove all tabs with spaces
-- nohup sed -i "s/\t\t*/ /g" /home/temp/visited_37.dat &
-- ------------------------------------------------------ --

COPY engine1.visited FROM '/home/temp/visited_37.dat' WITH delimiter = '\t';
