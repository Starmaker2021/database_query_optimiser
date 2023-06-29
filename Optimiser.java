package sjdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Optimiser {
//Heuristic optimisation strategies:
    //Push σ operators down the tree
    //Introduce joins (combine × and σ to create ⋈)
    //Determine join order
    //Push π operators down the tree

//Moving select and project down the tree：
    //First, record the information of select and project
    //Because of the canonical form, the bottom layer is product
    //So the moving of project and select should be done in the processing of product
    //For the relationship after scan, first do the select, and  do the project
    //The execution of select is divided into two situations:
    //1. attr = value
    //2. attr = attr (both attrs belong to the relation, for example, both a1 and a2 belong to A)

//Generation and sorting of joins:
    //Pair the relationships after select and project
    //Loop to generate the combination with the smallest cost, as the parent node of the two operators
    //The parent node is used as the left node, and then paired with the next operator to generate a new parent node
    //Until all the relationships are paired, generate a left deep tree with the most restrictive relations first


//Optimizer optimization detailed steps:
    //1. According to the standard structure, decompose layer by layer until the bottom product (AxBxC)
        //1.1. Create a projectDownList to assist in the moving down of the project
            //1.1.1.projectDownList records the attributes that the lower layer needs to project to the upper layer
            //1.1.2.AttributeWithCount stored in the ProjectDownList recording times of attribute appears in upper layers
            //1.1.3.IncrementCount method, used to increase the count of the corresponding attribute in AttributeWithCount
            //1.1.4.DecrementCount method, used to reduce the count of the corresponding attribute in AttributeWithCount
        //1.2. Create a selectDownList to record the select predicates
            //1.2.1. record attr = value
            //1.2.2. record attr = attr (attrs from the same relations,a1 = a2, a1,a2 belongs to A)
            //1.2.3. record attr = attr (attrs from different relations, a1 = b1, a1 belongs to A, b1 belongs to B)
    //2. Process product(AxBxC)
        //2.1. Preparation:
            //2.1.1. Create TreeNode class to store and operate optimized relation
            //2.1.2. Create LeftDeepNode class to store TreeNode and Predicate in pairs
            //2.1.3. Create an arraylist pairs to store LeftDeepNode
            //2.1.4. Create an arraylist join to store TreeNodes that can create joins with other TreeNodes
            //2.1.5. Create an arraylist product to store TreeNodes that can only create products with other TreeNodes
        //2.2. Recursively process product(AxBxC), one relation each time
            //2.2.1. For each relation, first call selectDown to move select down according to selectDownList
            //2.2.2. According to the projectDownList, call the projectDown method to move the project down
            //2.2.3. Check whether a join can be created according to selectDownList
                //2.2.3.1. If no, directly add to the product list
                //2.2.3.2. If yes, add to the join list, and generate or add to the LeftDeepNode in pairs
            //2.2.4.TreeNodes in the product list and the TreeNode in the join list generate LeftDeepNode adding to pairs
            //2.2.5.TreeNodes in the product pair other TreeNodes in the product generate LeftDeepNode adding to pairs
    //3. Generate a new Join with the smallest cost in turn, and generate a left deep tree
        //3.1 Traverse the pairs, find the LeftDeepNode with the smallest cost, create a join between them
        //3.2 The newly generated TreeNode is set as the parentNode of the two TreeNodes in LeftDeepNode.
        //3.3 When other TreeNodes want to pair with these two TreeNodes, they are directly paired with their parentNode
        //3.4.When the parentNode is paired with other TreeNodes, it will be used as a left node
        //3.5.Loop generation until the pairs are empty, generate a left deep tree with most restrictive relations first
        //3.6.Finally, perform a projectDown again, project the required attributes, and the optimization is completed

    Optimiser() {
    }

    Optimiser(Catalogue cat) {
    }

    Estimator estimator = new Estimator();

    //Create a projectDownList to assist in the moving down of the project
    ArrayList<AttributeWithCount> projectDownList = new ArrayList<>();

    //Create a selectDownList to assist in the moving down of the select after the scan
    //Also record the predicate used to generate the join
    ArrayList<Predicate> selectDownList = new ArrayList<>();


    //Decomposing a canonical query
    public Operator optimise(Operator plan) {
        //Decompose the top project
        if (plan instanceof Project) {
            //Decompose the top layer: project
            return projectProcess((Project) plan);
        } else if (plan instanceof Select) {
            //Decompose the second layer: select
            return selectProcess((Select) plan);
        } else if (plan instanceof Product) {
            //Decompose the bottom layer: product
            return treeNodeProcess((Product) plan);
        }
        return null;
    }

    //Decompose the top project
    public Operator projectProcess(Project op) {
        Operator optimisedResult;
        //Record the attributes that need to be projected to the upper layer
        for (Attribute a : op.getAttributes()) {
            incrementCount(projectDownList, a);
        }
        //After the lower level operation is complete, project again, projecting the required attributes
        optimisedResult = projectDown(optimise(op.getInput()));
        return optimisedResult;
    }

    //Decompose the second layer: select
    public Operator selectProcess(Select op) {
        //Record the attributes in projectDownList
        Operator optimisedResult;
        Attribute leftAttr = op.getPredicate().getLeftAttribute();
        incrementCount(projectDownList, leftAttr);
        //if the predicate is attr = attr, record the right attribute as well
        if (!op.getPredicate().equalsValue()) {
            Attribute rightAttr = op.getPredicate().getRightAttribute();
            incrementCount(projectDownList, rightAttr);
        }
        //record the predicate in selectDownList
        selectDownList.add(op.getPredicate());
        optimisedResult = optimise(op.getInput());
        return optimisedResult;
    }

    //Decompose the bottom layer: product
    public Operator treeNodeProcess(Product op) {
        //Arraylist pairs stores the LeftDeepNode that may generate a new join or product
        ArrayList<LeftDeepNode> pairs = new ArrayList<>();
        //Arraylist joins stores TreeNodes that can generate joins
        ArrayList<TreeNode> joins = new ArrayList<>();
        //Arraylist products stores the TreeNode that can only generate products
        ArrayList<TreeNode> products = new ArrayList<>();

        //The createJoinPairs method is called to process the input op in the form of Product
        //Call recursive to split the product into individual relations and then move select and project down
        //Generate a new op instance and add it to joins or products
        //Generate a LeftDeepNode of the part that can generate joins and add it to pairs
        createJoinPairs(op, pairs, joins, products);

        //TreeNode processing is not yet complete...
        //Currently only TreeNodes in joins are added to pairs, TreeNode in products has not yet been added to pairs
        //So next, the TreeNode in products needs to be added to pairs after it has been generated as a LeftDeepNode
        //with other TreeNodes in products or TreeNodes in joins respectively
        Operator optimisedResult = null;
        Iterator<TreeNode> productNode = products.iterator();
        while (productNode.hasNext()) {
            TreeNode tempNode = productNode.next();
            //create new LeftDeepNode with TreeNode in joins and add to pairs
            if (!joins.isEmpty()) {
                for (TreeNode joinsNode : joins) {
                    Predicate fake = new Predicate(null, "");
                    LeftDeepNode newLdp = new LeftDeepNode();
                    newLdp.setLeft(tempNode);
                    newLdp.setRight(joinsNode);
                    newLdp.setPredicate(fake);
                    pairs.add(newLdp);
                }
            }
            //create new LeftDeepNode with TreeNode in products and add to pairs
            for (int i = 1; i < products.size(); i++) {
                Predicate fake = new Predicate(null, "");
                LeftDeepNode newLdp = new LeftDeepNode();
                newLdp.setLeft(tempNode);
                newLdp.setRight(products.get(i));
                newLdp.setPredicate(fake);
                pairs.add(newLdp);
            }
            productNode.remove();
        }

        //Multiple rounds of traversing pairs, each round to select a LeftDeepNode that can generate
        //the least-cost join, generate a join as the left node, and then generate a new join with
        //the join generated in the next round, and so on, and finally generate a left-deep tree.
        while (!pairs.isEmpty()) {
            Iterator<LeftDeepNode> pair = pairs.iterator();
            //The minCostNode is used to store the LeftDeepNode with the least cost
            LeftDeepNode minCostNode = null;
            LeftDeepNode swapTemp;

            while (pair.hasNext()) {
                LeftDeepNode tempNode = pair.next();
                Predicate tempPred = tempNode.getPredicate();
                Operator tempLeft = tempNode.getLeft().getNode();
                Operator tempRight = tempNode.getRight().getNode();
                Operator tempOutput;

                //If the left or right node is null, the LeftDeepNode is removed from pairs
                if (tempLeft == null || tempRight == null) {
                    pair.remove();
                    continue;
                } else if (tempPred.getLeftAttribute() == null) {
                    //If the predicate is null, it means the predicate is fake
                    //so create a new product with the left and right node
                    if (tempNode.getLeft().parentNode != null) {
                        //If the left node is a parentNode, the product is generated with it as the left node
                        tempOutput = new Product(tempLeft, tempRight);
                    } else {
                        //If not, the product is generated with the right node as the left node
                        tempOutput = new Product(tempRight, tempLeft);
                    }
                    estimator.visit((Product) tempOutput);
                    //record the new product in to compare with results of other LeftDeepNode
                    swapTemp = new LeftDeepNode(tempPred, tempNode.getLeft(), tempNode.getRight());
                } else if (tempLeft.equals(tempRight)) {
                    //If the left and right node have the same parentNode, generate a select with the predicate
                    tempOutput = new Select(tempLeft, tempPred);
                    estimator.visit((Select) tempOutput);
                    //record the new product in to compare with results of other LeftDeepNode
                    swapTemp = new LeftDeepNode(tempPred, tempNode.getLeft(), tempNode.getRight());
                } else {
                    //If the left and right node have different parentNode, generate a join with the predicate
                    List<Attribute> attrAll = tempLeft.getOutput().getAttributes();
                    if (attrAll.contains(tempPred.getLeftAttribute())) {
                        //if the left attribute of the predicate is in attributes of left node
                        //generate a join with the left node of LeftDeepNode as the left node
                        if (tempNode.getLeft().parentNode != null) {
                            //If the left node is a parentNode
                            //left attribute and right attribute of predicate do not need to be swapped
                            tempOutput = new Join(tempLeft, tempRight, tempPred);
                        }else {
                            //If not, left attribute and right attribute of predicate need to be swapped
                            //left node and right node of LeftDeepNode also need to be swapped
                            Predicate swapPred = new Predicate(tempPred.getRightAttribute(), tempPred.getLeftAttribute());
                            tempOutput = new Join(tempRight, tempLeft, swapPred);
                        }
                        estimator.visit((Join) tempOutput);
                        //record the new product in to compare with results of other LeftDeepNode
                        swapTemp = new LeftDeepNode(tempPred, tempNode.getLeft(), tempNode.getRight());
                    } else {
                        //if the left attribute of the predicate is in attributes of right node
                        //generate a join with the right node of LeftDeepNode as the left node
                        if (tempNode.getLeft().parentNode != null) {
                            //If the left node is a parentNode
                            //left attribute and right attribute of predicate do not need to be swapped
                            tempOutput = new Join(tempRight, tempLeft, tempPred);
                        }else {
                            //If not, left attribute and right attribute of predicate need to be swapped
                            //left node and right node of LeftDeepNode also need to be swapped
                            Predicate swapPred = new Predicate(tempPred.getRightAttribute(), tempPred.getLeftAttribute());
                            tempOutput = new Join(tempLeft, tempRight, swapPred);
                        }
                        estimator.visit((Join) tempOutput);
                        //record the new product in to compare with results of other LeftDeepNode
                        swapTemp = new LeftDeepNode(tempPred, tempNode.getRight(), tempNode.getLeft());
                    }
                }

                //If the cost of the swaptemp is less than the cost of the minCostNode or the minCostNode is null
                //The new join or product will replace the current minCostNode
                if (minCostNode == null || tempOutput.getOutput().getTupleCount() < optimisedResult.getOutput().getTupleCount()) {
                    minCostNode = swapTemp;
                    optimisedResult = tempOutput;
                }
            }

            //In the case of product generation, there is no need to
            // perform the following operations for joins in pairs
            if (minCostNode == null || minCostNode.getPredicate() == null) {
                continue;
            }

            Predicate pred = minCostNode.getPredicate();
            if (pred.getLeftAttribute() == null) {
                //For the TreeNode that generates the product, set it to "optimised"
                //so that the next time you get a TreeNode, it will return null,
                //preventing incorrect products.
                if (minCostNode.getLeft().nodeType.equals("Product")) {
                    minCostNode.getLeft().setNodeState("optimised");
                }
                if (minCostNode.getRight().nodeType.equals("Product")) {
                    minCostNode.getLeft().setNodeState("optimised");
                }
            } else {
                //For attributes that were already projected when the join was generated
                //remove them from the projectDownList
                decrementCount(projectDownList, pred.getLeftAttribute());
                decrementCount(projectDownList, pred.getRightAttribute());
            }

            //Remove the LeftDeepNode with the least cost from pairs
            Iterator<LeftDeepNode> removePair = pairs.iterator();
            while (removePair.hasNext()) {
                LeftDeepNode tempNode = removePair.next();
                if (tempNode.getPredicate().equals(minCostNode.getPredicate())) {
                    removePair.remove();
                    break;
                }
            }

            //Project of the newly generated join or product
            optimisedResult = projectDown(optimisedResult);
            TreeNode optimisedTarget = new TreeNode(optimisedResult);

            //Set the new TreeNode as the parentNode of the left and right nodes which involved in the join or product
            if (minCostNode.getLeft().getNode() == minCostNode.getRight().getNode()) {
                //If the left and right nodes have the same parentNode, then only one of them
                //will have a new parentNode, otherwise it will enter a wireless loop
                minCostNode.getLeft().setParentNode(optimisedTarget);
            } else {
                minCostNode.getLeft().setParentNode(optimisedTarget);
                minCostNode.getRight().setParentNode(optimisedTarget);
            }
        }
        return optimisedResult;
    }

    //Recursive processing of the lowest level product
    public void createJoinPairs(Operator op, ArrayList<LeftDeepNode> pairs, ArrayList<TreeNode> joins, ArrayList<TreeNode> products) {
        //Separate products by recursion up to a single relation
        if (op instanceof Product) {
            createJoinPairs(((Product) op).getLeft(), pairs, joins, products);
            createJoinPairs(((Product) op).getRight(), pairs, joins, products);
        } else {
            //First call the selectDown method to move select down
            //Then call the projectDown method to move project down
            Operator optimizedOp = projectDown(selectDown((Scan) op));

            //Create a new TreeNode to store the optimized operator
            TreeNode leftNode = new TreeNode(optimizedOp);
            List<Attribute> attrAll = optimizedOp.getOutput().getAttributes();

            //If no attr = attr predicate in selectDownList, that means only products can be generated.
            if (selectDownList.size() == 0) {
                leftNode.setNodeType("Product");
                products.add(leftNode);
                return;
            }

            //Iterate through the selectDownList to determine if the TreeNode can generate joins.
            Iterator<Predicate> predicateIterator = selectDownList.iterator();
            //The default setting is a product type TreeNode
            leftNode.setNodeType("Product");
            while (predicateIterator.hasNext()) {
                Predicate pred = predicateIterator.next();
                Attribute predLeft = pred.getLeftAttribute();
                Attribute predRight = pred.getRightAttribute();
                //The flag is used to determine whether the TreeNode has been added to pairs
                boolean flag;
                //If the predicate contains the attribute of the current TreeNode, the TreeNode type change to join
                if (attrAll.contains(predLeft)||attrAll.contains(predRight)) {
                    //If the TreeNode is not in the join list, add it to the join list
                    boolean inJoins = false;
                    for (TreeNode node : joins) {
                        if (node == leftNode) {
                            inJoins = true;
                        }
                    }
                    if (!inJoins) {
                        leftNode.setNodeType("Join");
                        joins.add(leftNode);
                    }
                }else {
                    //If it is a product type TreeNode, skip next operations
                    continue;
                }

                //add the join type TreeNode to pairs
                //if the pairs is empty, create a new leftDeepNode and add it to pairs
                if (pairs.size() == 0) {
                    LeftDeepNode newNode = new LeftDeepNode(pred, leftNode);
                    flag = true;
                    pairs.add(newNode);
                } else {
                    //iterate through the pairs to determine if the predicate is already in the pairs
                    flag = false;
                    Iterator<LeftDeepNode> pairIterator = pairs.iterator();
                    while (pairIterator.hasNext()) {
                        LeftDeepNode tempNode = pairIterator.next();
                        //if the predicate is already in the pairs, add the TreeNode to the leftDeepNode
                        if (tempNode.getPredicate().equals(pred)) {
                            tempNode.setRight(leftNode);
                            flag = true;
                            break;
                        }
                    }
                }
                //if the predicate is not in the pairs, create a new leftDeepNode and add it to pairs
                if (!flag) {
                    LeftDeepNode newNode = new LeftDeepNode(pred, leftNode);
                    pairs.add(newNode);
                }
            }
            //If the TreeNode is a product type, add it to the products
            if (leftNode.getNodeType().equals("Product")) {
                products.add(leftNode);
            }
        }
    }

    //The AttributeWithCount class is used to record the elements that need to be projected
    // and the number of occurrences during the top-down processing of a canonical query.
    public class AttributeWithCount {
        private Attribute attribute;
        private int count;

        public AttributeWithCount(Attribute attribute, int count) {
            this.attribute = attribute;
            this.count = count;
        }

        public Attribute getAttribute() {
            return attribute;
        }

        public void setAttribute(Attribute attribute) {
            this.attribute = attribute;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    //incrementCount function is used to increment the number of an attribute in the projectDownList array.
    public void incrementCount(ArrayList<AttributeWithCount> projectDownList, Attribute a) {
        boolean found = false;
        //Iterate through the projectDownList and if found, add count+1
        for (AttributeWithCount attrWithCount : projectDownList) {
            if (attrWithCount.getAttribute().equals(a)) {
                attrWithCount.setCount(attrWithCount.getCount() + 1);
                found = true;
                break;
            }
        }
        // If not found, create a new AttributeWithCount object and add it to the projectDownList
        if (!found) {
            projectDownList.add(new AttributeWithCount(a, 1));
        }
    }

    //decrementCount function is used to reduce the number of an attribute in the projectDownList array.
    public void decrementCount(ArrayList<AttributeWithCount> projectDownList, Attribute a) {
        //Iterate through the projectDownList and if found, set count-1
        for (AttributeWithCount attrWithCount : projectDownList) {
            if (attrWithCount.getAttribute().equals(a)) {
                attrWithCount.setCount(attrWithCount.getCount() - 1);
                //If count is 0, remove the property from the projectDownList
                if (attrWithCount.getCount() == 0) {
                    projectDownList.remove(attrWithCount);
                }
                break;
            }
        }
    }

    //The projectDown method is used to move the project down
    public Operator projectDown(Operator op) {
        ArrayList<Attribute> match = new ArrayList<>();
        List<Attribute> attrAll = op.getOutput().getAttributes();
        // Iterate through attrAll and add the attributes that will be used in subsequent processing to the match
        for (AttributeWithCount a : projectDownList) {
            if ((attrAll.contains(a.getAttribute()))) {
                if (match.contains(a.getAttribute())) {
                    continue;
                }
                match.add(a.getAttribute());
            }
        }
        //If match is empty, that means no attributes will be used and return directly to the original op
        //If some attributes will be used, call the project() method and project the properties in the match
        //If all properties will be used, return the op directly
        if (attrAll.size() > match.size()) {
            if (match.isEmpty()) {
                return op;
            }
            op = new Project(op, match);
            estimator.visit((Project) op);
        }
        return op;
    }

    //The selectDown method is used to move select down
    public Operator selectDown(Scan op) {
        List<Attribute> attrAll = op.getRelation().getAttributes();
        Operator originScan = new Scan((NamedRelation) op.getRelation());
        estimator.visit((Scan) originScan);
        Operator optimisedResult = originScan;
        //The selectDown here has two cases
        //1.attr = val
        //2.attr = attr (both attr's come from the same relation, e.g. a1 = a2, both a1 and a2 come from A)
        Iterator<Predicate> selectIterator = selectDownList.iterator();
        while (selectIterator.hasNext()) {
            Predicate pred = selectIterator.next();
            for (int j = 0; j < attrAll.size(); j++) {
                if (pred.getLeftAttribute().equals(attrAll.get(j))) {
                    //If the predicate is attr = val, add the predicate to the optimisedResult
                    if (pred.getRightAttribute() == null) {
                        optimisedResult = new Select(optimisedResult, pred);
                        estimator.visit((Select) optimisedResult);
                        selectIterator.remove();
                        // Call the decrement method to remove attributes from the projectDownList
                        // prevent requests to project attributes that do not exist
                        decrementCount(projectDownList, pred.getLeftAttribute());
                    }
                    //If the predicate is attr = attr, add the predicate to the optimisedResult
                    if (attrAll.contains(pred.getRightAttribute())) {
                        optimisedResult = new Select(optimisedResult, new Predicate(pred.getLeftAttribute(), pred.getRightAttribute()));
                        estimator.visit((Select) optimisedResult);
                        selectIterator.remove();
                        // Call the decrement method to remove attributes from the projectDownList
                        // prevent requests to project attributes that do not exist
                        decrementCount(projectDownList, pred.getLeftAttribute());
                        decrementCount(projectDownList, pred.getRightAttribute());
                    }
                }
            }
        }
        return optimisedResult;
    }

    //The TreeNode class is used to store the operator after the initial optimization.
    public class TreeNode {

        private Operator node;

        //The TreeNode has a nodeType property, which is used to record
        // whether the TreeNode is used to generate a product or a join.
        private String nodeType;

        //TreeNode has a parent property, when a node joins with an optimised deep left tree,
        //it will only join with the top parent, so the parent property helps with this process
        private String nodeState = "unoptimised";

        //TreeNode has a parenNode property, when a node joins with an optimised left deep tree,
        // it will only join with the top parentNode, so the parentNode attribute helps with this process
        public TreeNode parentNode;

        public TreeNode(Operator op) {
            this.node = op;
        }

        //The return value of the getNode method of a TreeNode has three cases
        //1. TreeNode that has been optimized returns null directly
        //2. TreeNode that is not optimised but has parentNode returns the operator of the top-level parentNode
        //3. TreeNode that has not been optimised but has no parent returns its own operator
        public Operator getNode() {
            if (this.getNodeState().equals("unoptimised")) {
                if (getParentNode() == null) {
                    return node;
                } else {
                    return getParentNode().getNode();
                }
            } else {
                return null;
            }
        }

        public void setNode(Operator node) {
            this.node = node;
        }

        public String getNodeType() {
            return nodeType;
        }

        public void setNodeType(String nodeType) {
            this.nodeType = nodeType;
        }

        public String getNodeState() {
            return nodeState;
        }

        public void setNodeState(String nodeState) {
            this.nodeState = nodeState;
        }

        public TreeNode getParentNode() {
            return parentNode;
        }

        public void setParentNode(TreeNode parentNode) {
            if (this.parentNode == null) {
                this.parentNode = parentNode;
            } else {
                this.parentNode.setParentNode(parentNode);
            }
        }
    }

    //LeftDeepNode is a class for storing TreeNode pairs that may generate joins,
    // including the join predicate between them
    public class LeftDeepNode {
        Predicate predicate;
        TreeNode left;
        TreeNode right;

        public LeftDeepNode() {
        }

        //The case where only one TreeNode is found when creating a new LeftNode.
        public LeftDeepNode(Predicate predicate, TreeNode left) {
            this.predicate = predicate;
            this.left = left;
        }

        //The case where two TreeNodes are found at the same time when a new LeftNode is created.
        public LeftDeepNode(Predicate predicate, TreeNode left, TreeNode right) {
            this.predicate = predicate;
            this.left = left;
            this.right = right;
        }

        public Predicate getPredicate() {
            return predicate;
        }

        public void setPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public TreeNode getLeft() {
            return left;
        }

        public void setLeft(TreeNode left) {
            this.left = left;
        }

        public TreeNode getRight() {
            return right;
        }

        public void setRight(TreeNode right) {
            this.right = right;
        }
    }
}
