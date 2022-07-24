package com.lapanthere.motion

/**
 * The metadata for a record that has been published to Kinesis.
 */
public interface Context {
    /**
     * Returns the property with the given name registered in the current record context, or `null`
     * if there is no property by that name.
     *
     * @param name a `String` specifying the name of the property.
     * @return an `Object` containing the value of the property, or `null` if no property exists matching the
     * given name.
     */
    public fun getProperty(name: String): Any?

    /**
     * Binds an object to a given property name in the current record context. If the name specified is
     * already used for a property, this method will replace the value of the property with the new value.
     *
     * @param name a `String` specifying the name of the property.
     * @param value an `Object` representing the property to be bound.
     */
    public fun setProperty(name: String, value: Any?)

    /**
     * Returns `true` if the property with the given name is registered in the current record
     * context, or `false` if there is no property by that name.
     *
     * @param name a `String` specifying the name of the property.
     * @return `true` if this property is registered in the context, or `false` if no property exists matching
     * the given name.
     */
    public fun hasProperty(name: String): Boolean = getProperty(name) != null

    /**
     * Removes a property with the given name from the current record context.
     * @param name a `String` specifying the name of the property to be removed.
     */
    public fun removeProperty(name: String)

    /**
     * Returns an immutable {@link Collection collection} containing the property names
     * available within the context of the current record context.
     *
     * @return an immutable {@link Collection collection} of property names.
     */
    public fun getPropertyNames(): Collection<String>
}

internal class RecordContext(
    val properties: MutableMap<String, Any?> = mutableMapOf()
) : Context {
    override fun getProperty(name: String): Any? = properties[name]

    override fun setProperty(name: String, value: Any?) {
        properties[name] = value
    }

    override fun removeProperty(name: String) {
        properties.remove(name)
    }

    override fun hasProperty(name: String): Boolean = properties.containsKey(name)

    override fun getPropertyNames(): List<String> = properties.keys.toList()
}
