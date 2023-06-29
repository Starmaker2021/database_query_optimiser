package sjdb;

import java.util.Iterator;

public class Estimator implements PlanVisitor {

    public Estimator() {
    }

    public void visit(Scan op) {
        Relation input = op.getRelation();
        Relation output = new Relation(input.getTupleCount());
        Iterator<Attribute> iter = input.getAttributes().iterator();
        while (iter.hasNext()) {
            output.addAttribute(new Attribute(iter.next()));
        }
        op.setOutput(output);
    }

    public void visit(Project op) {
        Relation input = op.getInput().getOutput();
        //T(πA(R)) = T(R)
        Relation output = new Relation(input.getTupleCount());
        for (Attribute attr : op.getAttributes()) {
            output.addAttribute(new Attribute(input.getAttribute(attr)));
        }
        op.setOutput(output);
    }


    public void visit(Select op) {
        Predicate pred = op.getPredicate();
        Relation input = op.getInput().getOutput();
        Relation output;
        int tupleCount;
        //attr = value
        if (op.getPredicate().equalsValue()) {
            Attribute leftAttr = input.getAttribute(pred.getLeftAttribute());
            //T(σA=c(R)) = T(R)/V(R,A)
            tupleCount = input.getTupleCount() / leftAttr.getValueCount();
            output = new Relation(tupleCount);
            for (Attribute inAttr : input.getAttributes()) {
                if (inAttr.equals(pred.getLeftAttribute())) {
                    //V(σA=c(R),A) = 1
                    output.addAttribute(new Attribute(inAttr.getName(), 1));
                } else {
                    output.addAttribute(new Attribute(inAttr));
                }
            }
        //attr = attr
        } else {
            Attribute leftAttr = input.getAttribute(pred.getLeftAttribute());
            Attribute rightAttr = input.getAttribute(pred.getRightAttribute());
            int leftCount = leftAttr.getValueCount();
            int rightCount = rightAttr.getValueCount();
            //T(σA=c(R)) = T(R)/max(V(R,A),V(R,B))
            tupleCount = input.getTupleCount() / Math.max(leftCount, rightCount);
            output = new Relation(tupleCount);

            for (Attribute inAttr : input.getAttributes()) {
                if (inAttr.equals(pred.getLeftAttribute()) || inAttr.equals(pred.getRightAttribute())) {
                    //V(σA=B(R),A) = V(σA=B(R),B) = min(V(R,A), V(R,B))
                    output.addAttribute(new Attribute(inAttr.getName(), Math.min(leftCount, rightCount)));
                } else {
                    output.addAttribute(new Attribute(inAttr));
                }
            }
        }
        op.setOutput(output);
    }

    public void visit(Product op) {
        Relation leftRela = op.getLeft().getOutput();
        Relation rightRela = op.getRight().getOutput();
        int leftCount = leftRela.getTupleCount();
        int rightCount = rightRela.getTupleCount();
        //T(R × S) = T(R)T(S)
        int tupleCount = leftCount * rightCount;
        Relation output = new Relation(tupleCount);
        for (Attribute attr : leftRela.getAttributes()) {
            output.addAttribute(new Attribute(attr));
        }
        for (Attribute attr : rightRela.getAttributes()) {
            output.addAttribute(new Attribute(attr));
        }
        op.setOutput(output);
    }

    public void visit(Join op) {
        Predicate pred = op.getPredicate();

        Relation leftRela = op.getLeft().getOutput();
        Relation rightRela = op.getRight().getOutput();
        int leftTupleCount = leftRela.getTupleCount();
        int rightTupleCount = rightRela.getTupleCount();

        Attribute leftAttr = leftRela.getAttribute(pred.getLeftAttribute());
        Attribute rightAttr = rightRela.getAttribute(pred.getRightAttribute());
        int leftValueCount = leftAttr.getValueCount();
        int rightValueCount = rightAttr.getValueCount();

        //max(V(R,A),V(S,B))
        int maxValue = Math.max(leftValueCount, rightValueCount);
        //min(V(R,A), V(S,B))
        int minValue = Math.min(leftValueCount, rightValueCount);
        //T(R⨝A=BS) = T(R)T(S)/max(V(R,A),V(S,B))
        int tupleCount = leftTupleCount * rightTupleCount / maxValue;
        Relation output = new Relation(tupleCount);
        //V(R⨝A=BS,A) = V(R⨝A=BS,B)=min(V(R,A), V(S,B)), V(R⨝A=BS,C) = V(R,C)
        for (Attribute attr : leftRela.getAttributes()) {
            if (attr.equals(pred.getLeftAttribute())) {
                output.addAttribute(new Attribute(attr.getName(), minValue));
            }else{
                output.addAttribute(new Attribute(attr));
            }
        }
        for (Attribute attr : rightRela.getAttributes()) {
            if (attr.equals(pred.getRightAttribute())) {
                output.addAttribute(new Attribute(attr.getName(), minValue));
            } else {
                output.addAttribute(new Attribute(attr));
            }
        }
        op.setOutput(output);
    }
}
