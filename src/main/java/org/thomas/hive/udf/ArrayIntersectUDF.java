package org.thomas.hive.udf;



import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;


/**
 * Return a  map of entries from a map, for a given set of keys.
 */



public class ArrayIntersectUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(ArrayIntersectUDF.class);
    private StandardListObjectInspector retValInspector;
    private ListObjectInspector[] listInspectorArr;


    private class InspectableObject implements Comparable {
        public Object o;
        public ObjectInspector oi;

        public InspectableObject(Object o, ObjectInspector oi) {
            this.o = o;
            this.oi = oi;
        }

        @Override
        public int hashCode() {
            ///LOG.info( "Object is " + o  + " :: " + o.getClass().getCanonicalName() + " Inspector = " + oi + " ::" + oi.getCategory() + " type =" + oi.getTypeName());
            return ObjectInspectorUtils.hashCode(o, oi);
        }

        @Override
        public int compareTo(Object arg0) {
            InspectableObject otherInsp = (InspectableObject) arg0;
            return ObjectInspectorUtils.compare(o, oi, otherInsp.o, otherInsp.oi);
        }

        @Override
        public boolean equals(Object other) {
            return compareTo(other) == 0;
        }

    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {

        HashMap checkSet = new HashMap();
        Object firstUndeferred = arg0[0].get();
        int firstArrSize = listInspectorArr[0].getListLength(firstUndeferred);
        for (int i = 0; i < firstArrSize; ++i) {
            Object unInspected = listInspectorArr[0].getListElement(firstUndeferred, i);
            InspectableObject io = new InspectableObject(unInspected, listInspectorArr[0].getListElementObjectInspector());
            checkSet.put(io, io);
        }
        for (int i = 1; i < arg0.length; ++i) {
            Object undeferred = arg0[i].get();
            HashMap newSet = new HashMap();
            for (int j = 0; j < listInspectorArr[i].getListLength(undeferred); ++j) {
                Object nonStd = listInspectorArr[i].getListElement(undeferred, j);
                InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[i].getListElementObjectInspector());
                if (checkSet.containsKey(stdInsp)) {
                    newSet.put(checkSet.get(stdInsp), checkSet.get(stdInsp));
                }
            }
            checkSet = newSet;
        }

        List retVal = (List) retValInspector.create(0);
        for (Object io : checkSet.keySet()) {
            InspectableObject inspObj = (InspectableObject) io;

            Object stdObj = ObjectInspectorUtils.copyToStandardObject(inspObj.o, inspObj.oi);
            retVal.add(stdObj);
        }
        return retVal;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "array_intersect(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length < 2) {
            throw new UDFArgumentException(" Expecting at least two arrays as arguments ");
        }
        ObjectInspector first = arg0[0];
        listInspectorArr = new ListObjectInspector[arg0.length];
        if (first.getCategory() == Category.LIST) {
            listInspectorArr[0] = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array as first argument ");
        }
        for (int i = 1; i < arg0.length; ++i) {
            if (arg0[i].getCategory() != Category.LIST) {
                throw new UDFArgumentException(" Expecting arrays arguments ");
            }
            ListObjectInspector checkInspector = (ListObjectInspector) arg0[i];
            if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector.getListElementObjectInspector())) {
                throw new UDFArgumentException(" Array types must match " + listInspectorArr[0].getTypeName() + " != " + checkInspector.getTypeName());
            }
            listInspectorArr[i] = checkInspector;
        }


        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }

}
