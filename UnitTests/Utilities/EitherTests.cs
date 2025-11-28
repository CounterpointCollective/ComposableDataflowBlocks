using System.Text.Json;
using CounterpointCollective.Utilities;
using Xunit;

namespace UnitTests.Utilities
{
    public class EitherTests
    {
        [Fact]
        public void TestEitherRoundTrip()
        {
            var c = Either<string, int>.Left("koeien");
            var o = new JsonSerializerOptions()
            //{
            //    Converters = { new EitherConverterFactory() }
            //};
            ;
            var s = JsonSerializer.Serialize(c, o);
            var c2 = JsonSerializer.Deserialize<Either<string, int>>(s, o);
            Assert.Equal(c, c2);
        }
    }
}
