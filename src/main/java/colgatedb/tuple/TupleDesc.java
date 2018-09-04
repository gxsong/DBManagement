package colgatedb.tuple;

import java.io.Serializable;
import java.util.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private TDItem[] TDItems;
    private int numFields = 0;
    private Map<String, Integer> fieldNameIndMap = new HashMap<>();


    /**
     * A helper class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final colgatedb.tuple.Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields of the
     * specified types and with associated names.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        //assume that typeAr.length == fieldAr.length
        init(typeAr, fieldAr);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types.  Field names should be assigned as empty
     * strings.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        String[] fieldAr = new String[typeAr.length];
        Arrays.fill(fieldAr, "");
        init(typeAr, fieldAr);

    }

    private void init(Type[] typeAr, String[] fieldAr){
        numFields = typeAr.length;
        TDItems = new TDItem[numFields];
        for(int i = 0; i < numFields; i++) {
            TDItems[i] = new TDItem(typeAr[i], fieldAr[i]);
            fieldNameIndMap.put(fieldAr[i], i);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return numFields;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i >= numFields)
            throw new NoSuchElementException("i is not a valid field reference: " + i);
        return TDItems[i].fieldType;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i >= numFields)
            throw new NoSuchElementException("i is not a valid field reference: " + i);
        return TDItems[i].fieldName;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        //assume that the field names are not empty strings
        if(!fieldNameIndMap.containsKey(name))
            throw new NoSuchElementException("no such field name!");
        return fieldNameIndMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.  The size
     * depends on the field types.
     *
     * @see Type#getLen()
     */
    public int getSize() {
        int size = 0;
        Iterator<TDItem> iterator = this.iterator();
        while(iterator.hasNext()){
            size += iterator.next().fieldType.getLen();
        }
        return size;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(TDItems).iterator();

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(o == null || !(o instanceof TupleDesc)) return false;
        if(o == this) return true;
        TupleDesc other = (TupleDesc) o;
        if(this.numFields != other.numFields)
            return false;
        Iterator<TDItem> thisIt = this.iterator();
        Iterator<TDItem> otherIt = other.iterator();
        while (thisIt.hasNext() && otherIt.hasNext()){
            TDItem thisItem = thisIt.next();
            TDItem otherItem = otherIt.next();
            if(!thisItem.fieldType.equals(otherItem.fieldType))
                return false;
        }
        return true;
    }

    public int hashCode() {
        int hashcode = 0;
        Iterator<TDItem> iterator = this.iterator();
        while (iterator.hasNext()){
            TDItem item = iterator.next();
            hashcode += item.fieldName.hashCode()/item.fieldType.hashCode();
        }
        return hashcode;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])".
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        Iterator<TDItem> iterator = iterator();
        while (iterator.hasNext()){
            TDItem item = iterator.next();
            result += (item.fieldName + "(" + item.fieldType + ")" + ", ");
        }
        result = result.substring(0, result.length()-2);
        return result;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int mergedSize = td1.numFields + td2.numFields;
        Type[] mergedTypes = new Type[mergedSize];
        String[] mergedFields = new String[mergedSize];
        Iterator<TDItem> it = td1.iterator();
        int i = 0;
        while (i < mergedSize){
            TDItem item = it.next();
            mergedTypes[i] = item.fieldType;
            mergedFields[i] = item.fieldName;
            i++;
            if(!it.hasNext())
                it = td2.iterator();
        }
        TupleDesc merged = new TupleDesc(mergedTypes, mergedFields);
        return merged;
    }


}
