# RDMA Mesh Exchange Architecture Diagram

## Network Topology (4-Node Example)

```
                    RDMA Cluster Network
                    192.168.100.0/24
                           │
        ┌──────────────────┼──────────────────┐──────────────────┐
        │                  │                  │                  │ 
┌───────▼────────┐ ┌───────▼────────┐ ┌───────▼────────┐ ┌───────▼────────┐
│   Node-A       │ │   Node-B       │ │   Node-C       │ │   Node-D       │
│ 192.168.100.10 │ │ 192.168.100.11 │ │ 192.168.100.12 │ │ 192.168.100.13 │
│  Rank: 0       │ │  Rank: 1       │ │  Rank: 2       │ │  Rank: 3       │
│  Port: 12340   │ │  Port: 12341   │ │  Port: 12342   │ │  Port: 12343   │
│                │ │                │ │                │ │                │
│ ┌────────────┐ │ │ ┌────────────┐ │ │ ┌────────────┐ │ │ ┌────────────┐ │
│ │ RDMA NIC   │ │ │ │ RDMA NIC   │ │ │ │ RDMA NIC   │ │ │ │ RDMA NIC   │ │
│ │ mlx5_0     │ │ │ │ mlx5_0     │ │ │ │ mlx5_0     │ │ │ │ mlx5_0     │ │
│ │ QP: 4096   │ │ │ │ QP: 4097   │ │ │ │ QP: 4098   │ │ │ │ QP: 4099   │ │
│ │ LID: 1     │ │ │ │ LID: 2     │ │ │ │ LID: 3     │ │ │ │ LID: 4     │ │
│ └────────────┘ │ │ └────────────┘ │ │ └────────────┘ │ │ └────────────┘ │
└────────────────┘ └────────────────┘ └────────────────┘ └────────────────┘
```

## Mesh Exchange Process Flow

### Server/Client Role Assignment

```
Rule: Lower-ranked node acts as SERVER for higher-ranked nodes

┌─────────────────┬──────────────────────────────────────┐
│ Node (Rank)     │ Role Assignment                      │
├─────────────────┼──────────────────────────────────────┤
│ Node-A (Rank 0) │ SERVER for Ranks 1, 2, 3             │
│ Node-B (Rank 1) │ SERVER for Ranks 2, 3                │
│                 │ CLIENT to Rank 0                     │
│ Node-C (Rank 2) │ SERVER for Rank 3                    │
│                 │ CLIENT to Ranks 0, 1                 │
│ Node-D (Rank 3) │ CLIENT to Ranks 0, 1, 2              │
└─────────────────┴──────────────────────────────────────┘
```

### TCP Connection Matrix for RDMA Info Exchange

```
                 ┌─────────────────────────────────────────┐
                 │          TCP Connections                │
                 │     (for RDMA info exchange)            │
                 └─────────────────────────────────────────┘

Node-A (0)      Node-B (1)      Node-C (2)      Node-D (3)
  [SRV]           [SRV]           [SRV]           
   │               │               │               
   │<──────────────┤               │              [CLI]
   │               │<──────────────┤               │
   │               │               │<──────────────┤
   │<──────────────┼───────────────┼───────────────┤
   │               │<──────────────┼───────────────┤
   │<──────────────┼───────────────┤               

Connections:
• Node-D → Node-A: TCP(192.168.100.13 → 192.168.100.10:12340)
• Node-D → Node-B: TCP(192.168.100.13 → 192.168.100.11:12341)  
• Node-D → Node-C: TCP(192.168.100.13 → 192.168.100.12:12342)
• Node-C → Node-A: TCP(192.168.100.12 → 192.168.100.10:12340)
• Node-C → Node-B: TCP(192.168.100.12 → 192.168.100.11:12341)
• Node-B → Node-A: TCP(192.168.100.11 → 192.168.100.10:12340)
```

### RDMA Information Exchange

```
Each TCP connection exchanges:

┌──────────────────┐     TCP Socket       ┌──────────────────┐
│ Lower Rank Node  │<─────────────────────│ Higher Rank Node │
│   (SERVER)       │                      │    (CLIENT)      │
│                  │                      │                  │
│ 1. Send RDMA Info│─────────────────────>│ 2. Recv RDMA Info│
│    • QP Number   │                      │                  │
│    • LID         │                      │                  │
│    • GID         │                      │                  │
│    • Memory Key  │                      │                  │
│                  │                      │                  │
│ 4. Recv RDMA Info│<─────────────────────│ 3. Send RDMA Info│
│                  │                      │                  │
└──────────────────┘                      └──────────────────┘
```

### Final RDMA Connection Matrix

```
After info exchange, each node has RDMA connection details for all other nodes:

Node-A (Rank 0) knows:          Node-B (Rank 1) knows:
├─ Node-B: QP=4097, LID=2      ├─ Node-A: QP=4096, LID=1
├─ Node-C: QP=4098, LID=3      ├─ Node-C: QP=4098, LID=3  
└─ Node-D: QP=4099, LID=4      └─ Node-D: QP=4099, LID=4

Node-C (Rank 2) knows:          Node-D (Rank 3) knows:
├─ Node-A: QP=4096, LID=1      ├─ Node-A: QP=4096, LID=1
├─ Node-B: QP=4097, LID=2      ├─ Node-B: QP=4097, LID=2
└─ Node-D: QP=4099, LID=4      └─ Node-C: QP=4098, LID=3

                    Full Mesh Achieved.
                All nodes can now establish 
             RDMA connections to any other node
```

## Timing Diagram

```
Time →
      0    2    4    6    8   10   12   14   16   18   20 seconds

Node-A │████│ Listen on :12340 ████████████████████████│
       │    │                                          │
Node-B │    │████│ Connect to A, Listen on :12341  ████│
       │    │    │                                     │  
Node-C │    │    │    │████│ Connect to A,B Listen :12342
       │    │    │    │    │                             
Node-D │    │    │    │    │    │████│ Connect to A,B,C 
       │    │    │    │    │    │    │                   
       │    │    │    │    │    │    │    Exchange Complete
       │    │    │    │    │    │    │    All nodes ready
       ▼    ▼    ▼    ▼    ▼    ▼    ▼    for RDMA ops
    Start Setup Setup Setup Setup Setup    ████████████
```

## Scalability Example (8 Nodes)

```
Total TCP Connections Required: n(n-1)/2 = 8×7/2 = 28 connections

Connection Distribution:
┌──────┬─────────────────────┬──────────────────────┐
│ Rank │ Acts as SERVER for  │ Acts as CLIENT to    │
├──────┼─────────────────────┼──────────────────────┤
│  0   │ 1,2,3,4,5,6,7 (7)   │ none                 │
│  1   │ 2,3,4,5,6,7   (6)   │ 0                    │
│  2   │ 3,4,5,6,7     (5)   │ 0,1                  │
│  3   │ 4,5,6,7       (4)   │ 0,1,2                │
│  4   │ 5,6,7         (3)   │ 0,1,2,3              │
│  5   │ 6,7           (2)   │ 0,1,2,3,4            │
│  6   │ 7             (1)   │ 0,1,2,3,4,5          │
│  7   │ none          (0)   │ 0,1,2,3,4,5,6        │
└──────┴─────────────────────┴──────────────────────┘

Balanced load: Each node makes roughly n/2 connections
No single point of failure: Fully distributed approach
```
