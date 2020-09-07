#ifndef _DEFS_H
#define _DEFS_H

#define MVP_BRANCHFACTOR 2   /* each level of internal node splits into branchfactor child nodes */
#define MVP_PATHLENGTH 16     /* no. vantage points in leaf node */
#define MVP_LEAFCAP 100       /* max. number points allowed in leaf node */
#define MVP_LEVELSPERNODE 6  /* number vantage points in internal node */

#define MVP_NUMSPLITS 32      /* max. number split points for last level of internal node */
                             /* bf^(levelspernode-1)                                     */
#define MVP_FANOUT 64         /* number child nodes to internal node: bf^(levelspernode)  */

#define MVP_SYNC 500         /* max. queue size before triggering adding it to the tree */

#endif /* _DEFS_H */
