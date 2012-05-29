/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2005  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.
*/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include "sha1.c"
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

//static char *mtpoint;
//static char *buf2;
char hash[300] = "";
char hashpaths[10000] = "";
int writecalled = 0;

static int hello_getattr(const char *path, struct stat *stbuf)
{
//printf("In hello_getattr\n");
    	int res;
        char size[20];
       // printf("path:%s\n",path);
        //path1 = malloc(5000);
        //printf("Allocated\n");
        //if(strcmp(path, "/") == 0)
          // sprintf(path1,"%s", mtpoint);
        //else
          // sprintf(path1,"%s%s", mtpoint, path);
        //printf("path1:%s\n",path1);i
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);

	res = lstat(new_path, stbuf);
        printf("Path:%s\n", path);
        char metapath[400] = "/home/hpkancha/dedup1/Metastore/";
        strcat(metapath, path);
        FILE *fsize = NULL;
        fsize = fopen(metapath, "r");
        if (fsize != NULL){
        fgets(size, 128, fsize);
        printf("Size:%s\n", size);
        stbuf->st_size = atoi(size);
        fclose(fsize);
         }

	if (res == -1)
		return -errno;
        //free(path1);
	return 0;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t 
filler,
                         off_t offset, struct fuse_file_info *fi)
{
printf("In readdir\n");
        DIR *dp;
	struct dirent *de;

	(void) offset;
	(void) fi;
        
        //path1 = malloc(5000);
       // printf("Allocated\n");
        //if(strcmp(path, "/") == 0)
          // sprintf(path1,"%s", mtpoint);
        //else
          // sprintf(path1,"%s%s", mtpoint, path);
        printf("path:%s\n",path);
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);


	dp = opendir(new_path);
	if (dp == NULL)
		return -errno;

	while ((de = readdir(dp)) != NULL) {
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0))
			break;
	}

	closedir(dp);
        //free(path1);
	return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
printf("In hello_open\n");
       
        if (*(path + 1) != '.'){
    	int res;
        printf("path:%s\n",path);
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);

	res = open(new_path, fi->flags);
	if (res == -1)
		return -errno;

	close(res);
       }
	return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t 
offset,
                      struct fuse_file_info *fi)
{
printf("In hello_read\n");
       if (*(path + 1) != '.'){
        printf("path:%s\n",path);
        //printf("Read size:%d\n", size);
        int fd;
        int res;
        int post = 0;       
        char *buf1 = NULL; 
        char datapath[100] = "";
        char dpath[100] = "";
        char cont[4096] = "";
        char size1[20];
        char metapath[400] = "/home/hpkancha/dedup1/Metastore/";
        strcat(metapath, path);
        FILE *fpath = NULL;
//printf("");
        fpath = fopen(metapath, "r");
        if (fpath != NULL){
        fgets(size1, 128, fpath);
        printf("Size:%d\n", atoi(size1));
        buf1 = malloc(atoi(size1));
        int i = 0;
        while (fgets(datapath, 100, fpath) != NULL){
	printf("datapath length is %d \n", strlen(datapath));
        for (i = 0; i < strlen(datapath); i++){
            if (datapath[i] != ',')
                 dpath[i] = datapath[i];
        }
        printf("Datapath:%s\n", datapath);
        printf("Before printing path:\n");
        printf("D Path:%s\n", dpath);
        printf("After printing path\n");
        FILE *fcont = NULL;
        fcont = fopen(dpath, "rb");
        printf("After fopen\n");
        fseek(fcont, 0, SEEK_END);
        printf("After fseek\n");
        long pos = ftell(fcont);
        printf("After pos\n");
        fseek(fcont, 0, SEEK_SET);
        printf("After fseek\n");
        fread(cont, pos, 1, fcont);
        printf("cont:\n",cont);
        printf("Before copy to buf\n");
        //strcat(buf, cont);
        if ((atoi(size1) - post) < 4096)
           pos = (atoi(size1) - post);
        memcpy(buf1 + post, cont, pos);
        printf("After copy to buf\n");
        fclose(fcont);
        printf("In if buf:%s\n", cont);
        post = post + pos;
        memset(dpath, 0, 100);
        memset(datapath, 0, 100);
        }
        fclose(fpath);        
        }

        printf("After if buf:%s\n", buf1);
        memcpy(buf, buf1 + offset, size);

	(void) fi;
	/*fd = open(path, O_RDONLY);
	if (fd == -1)
		return -errno;

        printf("After if buf:%s\n", buf);

	res = pread(fd, buf, size, offset);
	if (res == -1)
		res = -errno;

	close(fd);*/
        free(buf1);
      
	return size;
      }
}

static int hello_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
       char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);
       printf("In hello_write\n");
       printf("path:%s\n",new_path);
	int fd, fd1, bytesRead;
	int res, res1;
       int index = 1;
       char pathname[50] = "";
       FILE *fdds = NULL;
       char hashpath[400] ="/home/hpkancha/dedup1/Datastore/";

	(void) fi;

        if (*(path + 1) != '.'){
        writecalled = 1;
	fd = open(new_path, O_WRONLY);
	if (fd == -1)
		return -errno;

       res = pwrite(fd, buf, size, offset);
       if (res == -1)
                res = -errno;

       //buf2 = malloc(size);
       //memcpy(buf2, buf, size);
       printf("Size:%d\n", size);
       printf("Offset:%d\n", offset);
       //printf("Buffer:%s\n", buf2);
       
       //fdds = fopen("metadata.txt", "a");
       hash1(buf);
       printf("%s\n", hash);
       strcat(hashpath, hash);
       printf("%s\n", hashpath);
       
       fdds = fopen(hashpath, "wb");
	if(fdds!=NULL){
       printf("After fopen\n");
       fprintf(fdds, "%s", buf);
       printf("After fprintf\n");
       fclose(fdds);
}
       strcat(hashpath, ",");
       
       printf("After adding comma\n");

       //hashpaths = realloc(hashpaths, strlen(hashpaths) + strlen(hashpath) + 5);
       //printf("Malloc Size:%d\n",  strlen(hashpaths) + strlen(hashpath) + 3);
       //printf("After Mallco\n");
       strcat(hashpaths, hashpath);
       printf("After hashpath\n");
       strcat(hashpaths, "\n");
       printf("After ,\n");
       printf("%s", hashpaths);

       //printf("fdbuf:%d\n", fdbuf);
       //fprintf(fdbuf,"%s","hi");
       //free(buf2);
      
       close(fd);
      }

	return res;
}

static int hello_mkdir(const char *path, mode_t mode)
{
        printf("In hello_mkdir");
        printf("path:%s\n",path);
	int res;
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);

	res = mkdir(new_path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int hello_unlink(const char *path)
{
        printf("In hello_unlink");
        printf("path:%s\n",path);
 	int res;
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);
        char metapath[400] = "/home/hpkancha/dedup1/Metastore/";
        strcat(metapath, path);
       
	res = unlink(new_path);
       res = unlink(metapath);
	if (res == -1)
		return -errno;

	return 0;
}

static int hello_rmdir(const char *path)
{
        printf("In hello_rmdir");
        printf("path:%s\n",path);
	int res;
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);
        char metapath[400] = "/home/hpkancha/dedup1/Metastore/";
        strcat(metapath, path);

	res = rmdir(new_path);
       res = rmdir(metapath);
	if (res == -1)
		return -errno;

	return 0;
}

static int hello_access(const char *path, int mask)
{
        return 0;
        printf("In hello_access");
        printf("path:%s\n",path);
	int res;
        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);

	res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int hello_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;

        char new_path[100] = "/home/hpkancha/dedup1";
        strcat(new_path, path);

	/* On Linux this could just be 'mknod(path, mode, rdev)' but this
	   is more portable */
	if (S_ISREG(mode)) {
		res = open(new_path, O_CREAT | O_EXCL | O_WRONLY, mode);
		if (res >= 0)
			res = close(res);
	} else if (S_ISFIFO(mode))
		res = mkfifo(new_path, mode);
	else
		res = mknod(new_path, mode, rdev);
	if (res == -1)
		return -errno;

	return 0;
}

static int hello_release(const char *path, struct fuse_file_info *fi)
{
       if (*(path + 1) != '.' && writecalled == 1){
       char new_path[100] = "/home/hpkancha/dedup1";
       strcat(new_path, path);

       writecalled = 0;
       struct stat st;
       stat(new_path, &st);
       int size = st.st_size;
       printf("Size:%d\n", size);
       printf("In hello_release\n");
	char metapath[400] ="/home/hpkancha/dedup1/Metastore";
        char pdirs[400] = "";
	strcat(metapath,path);
        char cmd[100] = "mkdir -p ";
	(void) path;
	(void) fi;
         int i;
         int index = 0;

       for (i = 0; i < strlen(metapath); i++){
           if (metapath[i] == '/'){
                 index = i;
           }      
       }

       printf("Index:%d\n", index);

       for (i = 0; i < index; i++){
            pdirs[i] = metapath[i];
       }

       printf("pdir:%s\n", pdirs);

       FILE *fdpaths = NULL; 

       printf("patth in relase is  %s\n", metapath);
       strcat(cmd, pdirs);
       system(cmd);
        fdpaths = fopen(metapath, "w");
       printf("%s\n", path);
       printf("After fopen\n");
       fprintf(fdpaths, "%d\n", size);
       fprintf(fdpaths, "%s", hashpaths);
       printf("Hashpaths:%s\n", hashpaths);
       printf("After fprintf\n");
       fclose(fdpaths);
       strcpy(hashpaths, "");
       
       FILE *forig = NULL;
       forig = fopen(new_path, "w");
       fprintf(forig, "");
       fclose(forig);
}
       return 0;
}

static struct fuse_operations hello_oper = {
    .getattr	  = hello_getattr,
    .access     = hello_access,
    .readdir	  = hello_readdir,
    .rmdir      = hello_rmdir,
    .open	  = hello_open,
    .read	  = hello_read,
    .write      = hello_write,
    .mkdir      = hello_mkdir,
    .unlink     = hello_unlink,
    .mknod      = hello_mknod,
    .release    = hello_release
};

void  hash1(char *str){
SHA1Context sha;
    int i;
   char y[300];
memset(hash, 0, 300);

SHA1Reset(&sha);
    SHA1Input(&sha, (const unsigned char *) str, strlen(str));
    if (!SHA1Result(&sha))
    {
        perror("ERROR-- could not compute message digest\n");
    }
    else
    {
        printf("\t");

       for(i = 0; i < 5 ; i++)
        {
              //printf("%X", sha.Message_Digest[i]);
              sprintf(y, "%X", sha.Message_Digest[i]);
		strcat(hash,y);
              memset(y,0,300);		
        }
   }
}

int main(int argc, char *argv[])
{
     //hashpaths = malloc(10000);
    // mtpoint = malloc(1000);
    //strcpy(mtpoint, argv[1]);
    //printf("\n%s\n", mtpoint);
    //chdir("/");
    //system("mkdir Datastore");
    //setenv("HOME", "/home/hpkancha/dedup2", 1);
    umask(0);
    return fuse_main(argc, argv, &hello_oper, NULL);
}




