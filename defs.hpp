#ifndef _DEFS_H
#define _DEFS_H

#define MVP_BRANCHFACTOR 2   /* each level of internal node splits into branchfactor child nodes */
#define MVP_PATHLENGTH 8     /* no. vantage points in leaf node */
#define MVP_LEAFCAP 30       /* max. number points allowed in leaf node */
#define MVP_LEVELSPERNODE 2  /* number vantage points in internal node */

#define MVP_NUMSPLITS 2      /* max. number split points for last level of internal node */
                             /* bf^(levelspernode-1)                                     */
#define MVP_FANOUT 4         /* number child nodes to internal node: bf^(levelspernode)  */

#define MVP_SYNC 500         /* max. queue size before triggering adding it to the tree */

#endif /* _DEFS_H */
