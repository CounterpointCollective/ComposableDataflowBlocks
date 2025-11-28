using System;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CounterpointCollective.Utilities
{
    [JsonConverter(typeof(EitherConverterFactory))]
    public record Either<A, B>
    {
        private readonly A? _left;
        private readonly B? _right;

        public A FromLeft => IsLeft ? _left! : throw new InvalidOperationException("I am right");

        public B FromRight => IsRight ? _right! : throw new InvalidOperationException("I am left");

        public bool IsLeft { get; private init; }
        public bool IsRight => !IsLeft;

        public Either(A left)
        {
            _left = left;
            IsLeft = true;
        }

        public Either(B right)
        {
            _right = right;
            IsLeft = false;
        }

#pragma warning disable CA1000 // Do not declare static members on generic types
        public static Either<A, B> Left(A left) => new(left);

        public static Either<A, B> Right(B right) => new(right);
#pragma warning restore CA1000 // Do not declare static members on generic types
    }

    public static class EitherExtensions
    {
        public static Either<C, B> SelectLeft<A, B, C>(this Either<A, B> e, Func<A, C> f) =>
            e.IsRight ? Either<C, B>.Right(e.FromRight) : Either<C, B>.Left(f(e.FromLeft));

        public static Either<A, C> SelectRight<A, B, C>(this Either<A, B> e, Func<B, C> f) =>
            e.IsRight ? Either<A, C>.Right(f(e.FromRight)) : Either<A, C>.Left(e.FromLeft);

        public static Either<A, B> Or<A, B>(this A? a, B b) =>
            a != null ? Either<A, B>.Left(a!) : Either<A, B>.Right(b);
    }

    public class EitherConverter<A, B> : JsonConverter<Either<A, B>>
    {
        public override Either<A, B>? Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options
        )
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new InvalidOperationException("StartObject expected");
            }
            _ = reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new InvalidOperationException("Property name expected");
            }
            var lr = reader.GetString();
            Either<A, B> ret;
            if (lr == "L")
            {
                _ = reader.Read();
                var v = JsonSerializer.Deserialize<A>(ref reader, options);
                ret = Either<A, B>.Left(v!);
            }
            else if (lr == "R")
            {
                _ = reader.Read();
                var v = JsonSerializer.Deserialize<B>(ref reader, options);
                ret = Either<A, B>.Right(v!);
            }
            else
            {
                throw new InvalidOperationException("Expected L or R");
            }

            _ = reader.Read();
            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new InvalidOperationException("EndObject expected");
            }
            return ret;
        }

        public override void Write(
            Utf8JsonWriter writer,
            Either<A, B> value,
            JsonSerializerOptions options
        )
        {
            if (value.IsLeft)
            {
                JsonSerializer.Serialize(writer, new { L = value.FromLeft }, options);
            }
            else
            {
                JsonSerializer.Serialize(writer, new { R = value.FromRight }, options);
            }
        }
    }

    public class EitherConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert) =>
            typeToConvert.IsGenericType
            && typeToConvert.GetGenericTypeDefinition() == typeof(Either<,>);

        public override JsonConverter? CreateConverter(
            Type typeToConvert,
            JsonSerializerOptions options
        )
        {
            var leftType = typeToConvert.GetGenericArguments()[0];
            var rightType = typeToConvert.GetGenericArguments()[1];

            var converter = (JsonConverter)
                Activator.CreateInstance(
                    typeof(EitherConverter<,>).MakeGenericType([leftType, rightType]),
                    BindingFlags.Instance | BindingFlags.Public,
                    binder: null,
                    args: null,
                    culture: null
                )!;
            return converter;
        }
    }
}
