/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg.vectors;

import com.facebook.airlift.log.Logger;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Custom implementation of VectorFloat that wraps a float[] array.
 * This is used because ArrayVectorFloat constructors are not public in JVector 4.0.0-rc.4.
 * Note: This class is not thread-safe. Concurrent modifications to the same instance
 * may result in unexpected behavior.
 * The vectorValue() method returns a defensive copy of the internal array to prevent
 * external modifications to the vector's data.
 *
 * This class also provides methods to convert to ArrayVectorFloat when needed.
 */
public class CustomVectorFloat
        implements VectorFloat<CustomVectorFloat>
{
    private static final Logger log = Logger.get(CustomVectorFloat.class);
    private final float[] values;
    private static Constructor<?> arrayVectorFloatConstructor;
    // Static initializer to find the ArrayVectorFloat constructor
    static {
        try {
            Class<?> arrayVectorFloatClass = Class.forName("io.github.jbellis.jvector.vector.ArrayVectorFloat");
            arrayVectorFloatConstructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
            arrayVectorFloatConstructor.setAccessible(true);
            log.info("Found ArrayVectorFloat constructor in static initializer");
        }
        catch (Exception e) {
            log.warn("Could not get ArrayVectorFloat constructor in static initializer: %s", e.getMessage());
            try {
                // Try to find the class from VectorFloat package
                Package vectorPackage = VectorFloat.class.getPackage();
                String packageName = vectorPackage.getName();
                // Try to load the class from the same package
                Class<?> arrayVectorFloatClass = Class.forName(packageName + ".ArrayVectorFloat");
                arrayVectorFloatConstructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
                arrayVectorFloatConstructor.setAccessible(true);
                log.info("Found ArrayVectorFloat constructor using package name: %s", packageName);
            }
            catch (Exception e2) {
                log.warn("Could not get ArrayVectorFloat constructor using package approach: %s", e2.getMessage());
                arrayVectorFloatConstructor = null;
            }
        }
    }

    public CustomVectorFloat(float[] values)
    {
        this.values = values;
    }
    /**
     * Attempts to convert this CustomVectorFloat to an ArrayVectorFloat instance.
     * This can be useful when JVector's internal code expects an ArrayVectorFloat.
     *
     * @return An ArrayVectorFloat instance if conversion is possible, or this instance if not
     */
    public VectorFloat<?> toArrayVectorFloat()
    {
        if (arrayVectorFloatConstructor != null) {
            try {
                // Create a copy of the values to ensure isolation
                float[] copy = new float[values.length];
                System.arraycopy(values, 0, copy, 0, values.length);
                // Create a new ArrayVectorFloat instance
                return (VectorFloat<?>) arrayVectorFloatConstructor.newInstance((Object) copy);
            }
            catch (Exception e) {
                log.warn("Failed to convert to ArrayVectorFloat: %s", e.getMessage());
            }
        }
        return this;
    }

    /**
     * Returns the vector instance.
     * Required by the VectorFloat interface.
     */
    @Override
    public CustomVectorFloat get()
    {
        return this;
    }

    @Override
    public float get(int index)
    {
        return values[index];
    }

    @Override
    public void set(int index, float value)
    {
        values[index] = value;
    }
    /**
     * Returns the length (dimension) of this vector.
     * This is required by the VectorFloat interface.
     */
    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public void copyFrom(VectorFloat src, int srcOffset, int destOffset, int length)
    {
        // If source is also a CustomVectorFloat, optimize the copy
        if (src instanceof CustomVectorFloat) {
            float[] srcValues = ((CustomVectorFloat) src).values;
            System.arraycopy(srcValues, srcOffset, this.values, destOffset, length);
        }
        else {
            // Otherwise, copy element by element
            for (int i = 0; i < length; i++) {
                this.values[destOffset + i] = src.get(srcOffset + i);
            }
        }
    }
    @Override
    public void zero()
    {
        for (int i = 0; i < values.length; i++) {
            values[i] = 0.0f;
        }
    }

    /**
     * Returns a hash code value for this vector.
     * This implementation matches the default in the VectorFloat interface.
     */
    @Override
    public int getHashCode()
    {
        int result = 1;
        for (int i = 0; i < this.length(); ++i) {
            if (this.get(i) != 0.0F) {
                result = 31 * result + Float.hashCode(this.get(i));
            }
        }
        return result;
    }

    /**
     * Returns the memory usage of this vector in bytes.
     * Required by the Accountable interface.
     */
    @Override
    public long ramBytesUsed()
    {
        // Base object overhead (16 bytes) + array reference (8 bytes) + float array size
        return 16 + 8 + (long) values.length * Float.BYTES;
    }

    /**
     * Returns a copy of the internal float array.
     * This method is not part of the VectorFloat interface in JVector 4.0.0-rc.3,
     * but is useful for our implementation.
     * @return A defensive copy of the internal float array
     */
    public float[] vectorValue()
    {
        float[] copy = new float[values.length];
        System.arraycopy(values, 0, copy, 0, values.length);
        return copy;
    }

    /**
     * Creates a copy of this vector with its own independent float array.
     * This method is not part of the VectorFloat interface in JVector 4.0.0-rc.3,
     * but is useful for our implementation.
     * @return A new CustomVectorFloat with a copy of the data
     */
    @Override
    public CustomVectorFloat copy()
    {
        float[] copy = new float[values.length];
        System.arraycopy(values, 0, copy, 0, values.length);
        return new CustomVectorFloat(copy);
    }
    /**
     * Returns the raw float array backing this vector.
     * This method is used by JVector's internal code in some cases.
     *
     * @return The internal float array (not a copy)
     */
    public float[] getFloatArray()
    {
        return values;
    }
    /**
     * Checks if this vector is compatible with JVector's internal code.
     * This can be used to determine if conversion to ArrayVectorFloat is needed.
     *
     * @return true if this vector is compatible, false otherwise
     */
    public boolean isCompatibleWithJVector()
    {
        try {
            // Try to find a method in JVector that might use our vector
            Class<?> vectorUtilClass = Class.forName("io.github.jbellis.jvector.vector.VectorUtil");
            for (Method method : vectorUtilClass.getDeclaredMethods()) {
                if (method.getName().equals("dotProduct")) {
                    // Found a method, try to invoke it with our vector
                    float[] testArray = new float[2];
                    testArray[0] = 1.0f;
                    testArray[1] = 2.0f;
                    CustomVectorFloat testVector = new CustomVectorFloat(testArray);
                    method.invoke(null, testVector, testVector);
                    return true;
                }
            }
        }
        catch (Exception e) {
            log.debug("Vector compatibility test failed: %s", e.getMessage());
            return false;
        }
        return false;
    }
}
