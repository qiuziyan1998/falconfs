#include "dfs.h"

using namespace std;


int dfs_init(int client_number)
{
    return 0;
}

int dfs_open(const char *path, int flags, mode_t mode)
{
    return open(path, flags, mode);
}

int dfs_read(int fd, void *buf, size_t count, off_t offset)
{
    return pread(fd, buf, count, offset);
}
int dfs_write(int fd, const void *buf, size_t count, off_t offset)
{
    return pwrite(fd, buf, count, offset);
}
int dfs_close(int fd, const char* path)
{
    return close(fd);
}
int dfs_mkdir(const char *path, mode_t mode)
{
    return mkdir(path, mode);
}
int dfs_rmdir(const char *path)
{
    return rmdir(path);
}
int dfs_create(const char *path, mode_t mode)
{
    return mknod(path, mode, 0);
}
int dfs_unlink(const char *path)
{
    return unlink(path);
}
int dfs_stat(const char *path, struct stat *stbuf)
{
    return stat(path, stbuf);
}

void dfs_shutdown()
{
    return;
}
