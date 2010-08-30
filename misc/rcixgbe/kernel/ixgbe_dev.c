/*
 * /dev/ node for mmaping registers into userspace
 */
#include <linux/pagemap.h>
#include <linux/types.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/vmalloc.h>
#include <linux/string.h>
#include <linux/miscdevice.h>

#include "ixgbe.h"
#include "ixgbe_dcb.h"
#include "ixgbe_dcb_82598.h"
#include "ixgbe_dcb_82599.h"
#include "ixgbe_common.h"
#include "ixgbe_dev.h"

#define MAX_ADAPTERS	32	/* reasonable high bound */

static struct registered_adapters {
	const char *name;
	struct ixgbe_adapter *adapter;
} registered_adapters[MAX_ADAPTERS];

static void
put_adapter(const char *name, struct ixgbe_adapter *adapter)
{
	int i;
	for (i = 0; i < MAX_ADAPTERS; i++) {
		if (registered_adapters[i].name == NULL) {
			registered_adapters[i].name = name;
			registered_adapters[i].adapter = adapter;
			return;
		}	
	}	
	BUG();
}

struct ixgbe_adapter *
get_adapter(const char *name)
{
	int i;
	for (i = 0; i < MAX_ADAPTERS; i++) {
		if (registered_adapters[i].name == NULL)
			continue;
		if (strcmp(registered_adapters[i].name, name) == 0)
			return registered_adapters[i].adapter;
	}
	return NULL;
}

static void
remove_adapter(const struct ixgbe_adapter *adapter)
{
	int i;
	for (i = 0; i < MAX_ADAPTERS; i++) {
		if (registered_adapters[i].adapter == adapter) {
			registered_adapters[i].name = NULL;
			registered_adapters[i].adapter = NULL;
			return;
		}
	}
	BUG();
}

static const char *
get_filename(struct inode *inode)
{
	struct dentry *dent;

	dent = list_first_entry(&inode->i_dentry, struct dentry, d_alias);
	if (dent == NULL)
		BUG();

	return dent->d_name.name;
}

static int
ixgbe_open(struct inode *inode, struct file *file)
{
	file->private_data = get_adapter(get_filename(inode));
	if (file->private_data == NULL)
		BUG();

	return 0;
}

// just return the mac address for now
static ssize_t
ixgbe_read(struct file *file, char __user *buffer,
    size_t buflen, loff_t *fpos)
{
	struct ixgbe_adapter *adapter = file->private_data;
	int err;

	if (buflen != 6 || *fpos != 0)
		return 0;

	err = copy_to_user(buffer, &adapter->hw.mac.perm_addr, 6); 
	if (err)
		return err;

	return 6;
}

static void
ixgbe_vm_open(struct vm_area_struct *vma)
{
}

static void
ixgbe_vm_close(struct vm_area_struct *vma)
{
}

static int
ixgbe_vm_fault(struct vm_area_struct *vma, struct vm_fault *vmf)
{
	BUG();
	return 0;
}

static struct vm_operations_struct ixgbe_mmap_ops = {
        .open  = ixgbe_vm_open,
        .close = ixgbe_vm_close,
	.fault = ixgbe_vm_fault
};

static int
ixgbe_mmap(struct file *file, struct vm_area_struct *vma)
{
	struct ixgbe_adapter *adapter = file->private_data;
	unsigned long start = vma->vm_start;
	unsigned long size  = vma->vm_end - vma->vm_start;
	unsigned long base  = pci_resource_start(adapter->pdev, 0);
	unsigned long pfn   = base >> PAGE_SHIFT;
	int err;

	if (vma->vm_pgoff)
		return -EINVAL;

	vma->vm_page_prot = pgprot_noncached(vma->vm_page_prot);
	err = remap_pfn_range(vma, start, pfn, size, vma->vm_page_prot);
	if (err)
		return err;

	printk(KERN_INFO "%s: [%s] mmaped device registers (0x%lx, %lu, %lu)\n",
	    __func__, current->comm, start, pfn, size);

	vma->vm_ops = &ixgbe_mmap_ops;
	return 0;
}

static struct file_operations ixgbe_miscdevice_fops = {
        .owner = THIS_MODULE,
        .open  = ixgbe_open,
	.read  = ixgbe_read,
        .mmap  = ixgbe_mmap,
};

int
ixgbe_dev_register(struct ixgbe_adapter *adapter)
{
	int err;
	const char *devname = dev_name(&adapter->pdev->dev);
	char *filename;

	filename = kmalloc(sizeof("ixgbe@") + strlen(devname), GFP_KERNEL);
	if (filename == NULL)
		return -ENOMEM;
	strcpy(filename, "ixgbe@"); 
	strcat(filename, devname); 

	adapter->miscdevice.minor = MISC_DYNAMIC_MINOR;
	adapter->miscdevice.name  = filename;
	adapter->miscdevice.fops  = &ixgbe_miscdevice_fops;

	err = misc_register(&adapter->miscdevice);
	if (err)
		return err;

	put_adapter(filename, adapter);
	return 0;
}

void
ixgbe_dev_unregister(struct ixgbe_adapter *adapter)
{
	misc_deregister(&adapter->miscdevice);
	kfree(adapter->miscdevice.name);
	remove_adapter(adapter);
}
