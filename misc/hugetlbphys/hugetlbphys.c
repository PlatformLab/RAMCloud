#include <linux/init.h>
#include <linux/module.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/sched.h>
#include <linux/proc_fs.h>

#include <asm/page.h>
#include <asm/pgtable.h>
#include <asm/io.h>

#define PML4E_PHYS_MASK 0x000ffffffffff000UL
#define PDPE_PHYS_MASK  0x000fffffc0000000UL

#define PRESENT_BIT	0x01
#define PAGE_SIZE_BIT	0x80

/*
 * Look up the physical address of the 1GB superpage located at
 * user virtual address ``vaddr''.
 *
 * Note that the user must access the page before calling to fault
 * the mapping in. If they don't, we won't find it in the page
 * table.
 */
static int
lookup_hugetlb_physaddr(loff_t vaddr, unsigned long *paddr)
{
	unsigned long pml4_offset = (vaddr >> 39) & 0x1ff;
	unsigned long pdp_offset  = (vaddr >> 30) & 0x1ff;
	unsigned long *pml4_base, *pdp_base;
	unsigned long pml4e, pdpe;

	// possible?
	if (!current->mm)
		return -EINVAL;

	pml4_base = (unsigned long *)current->mm->pgd;
	pml4e = pml4_base[pml4_offset];
	if ((pml4e & PRESENT_BIT) == 0)
		return -EINVAL;

	pdp_base = phys_to_virt(pml4e & PML4E_PHYS_MASK);
	pdpe = pdp_base[pdp_offset];
	if ((pdpe & PRESENT_BIT) == 0)
		return -EINVAL;

	if ((pdpe & PAGE_SIZE_BIT) == 0)
		return -EINVAL;

	*paddr = pdpe & PDPE_PHYS_MASK;
	return 0;
}

static ssize_t
hugetlbphys_read(struct file *file, char __user *buffer, size_t buflen,
    loff_t *fpos)
{
	unsigned long addr;
	int ret;

	printk("KERN_INFO %s: [%s] looking up superpage at vaddr 0x%lx\n",
	    __func__, current->comm, (unsigned long)*fpos);

	if (buflen != sizeof(addr))
		return -EINVAL;

	ret = lookup_hugetlb_physaddr(*fpos, &addr);
	if (ret)
		return ret;

	if (copy_to_user(buffer, &addr, sizeof(addr)))
		return -EFAULT;

	return sizeof(addr);
}

static int
hugetlbphys_open(struct inode *inode, struct file *filp)
{
	mutex_lock(&inode->i_mutex);
	i_size_write(inode, (loff_t)-1);
	mutex_unlock(&inode->i_mutex);
	return 0;
}

static const struct file_operations proc_hugetlbphys_operations = {
	.read = hugetlbphys_read,
	.open = hugetlbphys_open
};

static int __init
hugetlbphys_init(void)
{
	if (proc_create("hugetlbphys", S_IRUSR, NULL,
	    &proc_hugetlbphys_operations) == NULL) {
		printk(KERN_ERR "Unable to register hugetlbphys file\n");
		return -ENOMEM;
	}

	printk(KERN_INFO "hugetlbphys: module loaded\n");

	return 0;
}

module_init(hugetlbphys_init);

static void __exit
hugetlbphys_exit(void)
{
	remove_proc_entry("hugetlbphys", NULL);
	printk(KERN_INFO "hugetlbphys: module unloaded\n");
}

module_exit(hugetlbphys_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Stanford RAMCloud");
MODULE_DESCRIPTION("Resolve hugetlb virtual addresses to physical addresses\n");
MODULE_VERSION("proc");
