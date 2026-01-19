"""
Stage 3: Graph Builder

Builds hierarchy graph and computes ultimate parents from immediate parent mappings.
"""

from typing import Dict, List, Set, Optional, Tuple
import pandas as pd
from collections import defaultdict, deque


class HierarchyGraphBuilder:
    """
    Builds directed graph of corporate hierarchies and computes ultimate parents.
    
    Handles:
    - Transitive parent relationships (A→B→C means C is ultimate parent of A)
    - Cycle detection (invalid hierarchies)
    - Multiple root nodes
    - Disconnected companies
    """
    
    def __init__(self, detect_cycles: bool = True):
        """
        Initialize the graph builder.
        
        Args:
            detect_cycles: Whether to detect and report cycles
        """
        self.detect_cycles = detect_cycles
        self.graph = defaultdict(list)  # child -> list of parents
        self.reverse_graph = defaultdict(list)  # parent -> list of children
        self.all_nodes = set()
    
    def build(self, parent_mappings: pd.DataFrame) -> pd.DataFrame:
        """
        Build hierarchy graph from parent mappings.
        
        Args:
            parent_mappings: DataFrame with columns:
                - child: Child company name (or OPERATOR_ID)
                - parent: Parent company name
                - confidence: Optional confidence score
                
        Returns:
            DataFrame with ultimate parent information:
                - company: Company name
                - immediate_parent: Direct parent
                - ultimate_parent: Top-level parent
                - hierarchy_path: Full path to ultimate parent
                - hierarchy_depth: Number of levels
                - has_cycle: Whether this company is in a cycle
        """
        # Build graph from mappings
        self._build_graph(parent_mappings)
        
        # Compute ultimate parents for all nodes
        results = []
        
        for company in self.all_nodes:
            ultimate_info = self._find_ultimate_parent(company)
            
            # Get immediate parent
            immediate_parent = self.graph[company][0] if self.graph[company] else "ULTIMATE"
            
            results.append({
                "company": company,
                "immediate_parent": immediate_parent,
                "ultimate_parent": ultimate_info["ultimate_parent"],
                "hierarchy_path": ultimate_info["path"],
                "hierarchy_depth": ultimate_info["depth"],
                "has_cycle": ultimate_info["has_cycle"]
            })
        
        return pd.DataFrame(results)
    
    def _build_graph(self, parent_mappings: pd.DataFrame):
        """Build internal graph structures from parent mappings."""
        self.graph.clear()
        self.reverse_graph.clear()
        self.all_nodes.clear()
        
        for _, row in parent_mappings.iterrows():
            child = row.get("child") or row.get("OPERATOR_ID") or row.get("company")
            parent = row.get("parent") or row.get("IMMEDIATE_PARENT")
            
            if pd.isna(child) or pd.isna(parent):
                continue
            
            # Add to node set
            self.all_nodes.add(child)
            
            # Skip ULTIMATE parents
            if parent == "ULTIMATE" or parent == child:
                continue
            
            self.all_nodes.add(parent)
            
            # Build forward and reverse edges
            if parent not in self.graph[child]:
                self.graph[child].append(parent)
            
            if child not in self.reverse_graph[parent]:
                self.reverse_graph[parent].append(child)
    
    def _find_ultimate_parent(self, company: str) -> Dict:
        """
        Find the ultimate parent of a company using BFS.
        
        Args:
            company: Company name to find ultimate parent for
            
        Returns:
            Dictionary with ultimate parent info
        """
        if company not in self.graph or not self.graph[company]:
            # No parent = is ultimate parent
            return {
                "ultimate_parent": company,
                "path": company,
                "depth": 0,
                "has_cycle": False
            }
        
        # BFS to find ultimate parent
        visited = set()
        path = [company]
        current = company
        has_cycle = False
        
        while current in self.graph and self.graph[current]:
            # Check for cycle
            if current in visited:
                has_cycle = True
                break
            
            visited.add(current)
            
            # Move to parent (take first if multiple)
            next_parent = self.graph[current][0]
            
            if next_parent == "ULTIMATE" or next_parent == current:
                break
            
            path.append(next_parent)
            current = next_parent
        
        ultimate = path[-1]
        path_str = " → ".join(path)
        depth = len(path) - 1
        
        return {
            "ultimate_parent": ultimate,
            "path": path_str,
            "depth": depth,
            "has_cycle": has_cycle
        }
    
    def get_children(self, company: str) -> List[str]:
        """
        Get all direct children (subsidiaries) of a company.
        
        Args:
            company: Company name
            
        Returns:
            List of child company names
        """
        return self.reverse_graph.get(company, [])
    
    def get_all_descendants(self, company: str) -> Set[str]:
        """
        Get all descendants (recursive subsidiaries) of a company.
        
        Args:
            company: Company name
            
        Returns:
            Set of all descendant company names
        """
        descendants = set()
        queue = deque([company])
        
        while queue:
            current = queue.popleft()
            
            for child in self.get_children(current):
                if child not in descendants:
                    descendants.add(child)
                    queue.append(child)
        
        return descendants
    
    def get_hierarchy_tree(self, root_company: str) -> Dict:
        """
        Get full hierarchy tree starting from a root company.
        
        Args:
            root_company: Root company name
            
        Returns:
            Nested dictionary representing hierarchy tree
        """
        def build_tree(company: str, visited: Set[str]) -> Dict:
            if company in visited:
                return {"name": company, "children": [], "cycle": True}
            
            visited.add(company)
            children = self.get_children(company)
            
            return {
                "name": company,
                "children": [build_tree(child, visited.copy()) for child in children],
                "cycle": False
            }
        
        return build_tree(root_company, set())
    
    def find_cycles(self) -> List[List[str]]:
        """
        Find all cycles in the hierarchy graph.
        
        Returns:
            List of cycles, where each cycle is a list of company names
        """
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: List[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for parent in self.graph.get(node, []):
                if parent == "ULTIMATE":
                    continue
                
                if parent not in visited:
                    if dfs(parent, path + [parent]):
                        return True
                elif parent in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(parent)
                    cycle = path[cycle_start:] + [parent]
                    cycles.append(cycle)
                    return True
            
            rec_stack.remove(node)
            return False
        
        for node in self.all_nodes:
            if node not in visited:
                dfs(node, [node])
        
        return cycles
    
    def get_statistics(self) -> Dict:
        """
        Get statistics about the hierarchy graph.
        
        Returns:
            Dictionary with graph statistics
        """
        total_nodes = len(self.all_nodes)
        total_edges = sum(len(parents) for parents in self.graph.values())
        
        # Find root nodes (ultimate parents)
        roots = [
            node for node in self.all_nodes 
            if not self.graph[node] or self.graph[node] == ["ULTIMATE"]
        ]
        
        # Find leaf nodes (no children)
        leaves = [
            node for node in self.all_nodes 
            if not self.reverse_graph[node]
        ]
        
        # Detect cycles
        cycles = self.find_cycles() if self.detect_cycles else []
        
        return {
            "total_companies": total_nodes,
            "total_relationships": total_edges,
            "root_companies": len(roots),
            "leaf_companies": len(leaves),
            "cycles_detected": len(cycles),
            "avg_children_per_parent": total_edges / len(roots) if roots else 0
        }


