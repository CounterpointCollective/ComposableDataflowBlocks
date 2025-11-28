using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CounterpointCollective
{
    /// <summary>
    /// Can be used just as record (immutable), with the benifit of being able
    /// to add [IgnoreMember] attributes to properties, when their values should be ignored
    /// in the equality comparison.
    /// </summary>
    public abstract class ValueObject
    {
        private List<FieldInfo>? fields;

        private List<PropertyInfo>? properties;

        public override int GetHashCode()
        {
            var hash = GetProperties()
                .Select(property => property.GetValue(this, null))
                .Aggregate(17, HashValue);

            return GetFields().Select(field => field.GetValue(this)).Aggregate(hash, HashValue);
        }

        public static int HashValue(int seed, object? value)
        {
            var currentHash = value?.GetHashCode() ?? 0;

            return (seed * 23) + currentHash;
        }

        public virtual bool Equals(ValueObject? other) => Equals(other as object);

        public static bool operator ==(ValueObject? left, ValueObject? right) =>
            !(left is null ^ right is null) && left?.Equals(right) != false;

        public static bool operator !=(ValueObject? left, ValueObject? right) => !(left == right);

        public override bool Equals(object? obj) =>
            obj != null
            && obj.GetType() == GetType()
            && GetProperties().All(p => PropertiesAreEqual(obj, p))
            && GetFields().All(f => FieldsAreEqual(obj, f));

        private bool PropertiesAreEqual(object obj, PropertyInfo p) =>
            Equals(p.GetValue(this, null), p.GetValue(obj, null));

        private bool FieldsAreEqual(object obj, FieldInfo f) =>
            Equals(f.GetValue(this), f.GetValue(obj));

        private IEnumerable<PropertyInfo> GetProperties() =>
            properties ??= GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => p.GetCustomAttribute<IgnoreMemberAttribute>() is null)
                .ToList();

        private IEnumerable<FieldInfo> GetFields() =>
            fields ??= GetType()
                .GetFields(BindingFlags.Instance | BindingFlags.Public)
                .Where(f => f.GetCustomAttribute<IgnoreMemberAttribute>() is null)
                .ToList();
    }

    [AttributeUsage(
        AttributeTargets.Property | AttributeTargets.Field,
        AllowMultiple = false,
        Inherited = true
    )]
    public sealed class IgnoreMemberAttribute : Attribute { }
}
