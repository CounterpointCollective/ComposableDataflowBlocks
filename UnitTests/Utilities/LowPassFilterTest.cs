using CounterpointCollective.Utilities;

namespace UnitTests.Utilities
{
    public class LowPassFilterTest
    {
        [Fact]
        public void TestIt()
        {
            var lp = new LowPassFilter(10);
            Assert.Equal(12, lp.Next(12));
            Assert.Equal(6, lp.Next(0));
            Assert.Equal(4, lp.Next(0));

            lp = new(5, 10.0);
            Assert.Equal(6, lp.Next(2.0));
            Assert.Equal(14 / 3.0, lp.Next(2.0));

            for (var i = 0; i < 4; i++)
            {
                lp.Next(1);
            }
            Assert.Equal(1, lp.Next(1));

            Assert.Equal(4, lp.Next(16));
        }

        [Fact]
        public void TestGetBuffer()
        {
            var lp = new LowPassFilter(3);
            lp.Next(1);
            lp.Next(2);
            lp.Next(3);

            Assert.Equal([1, 2, 3], lp.GetBuffer());
            lp.Next(4);
            Assert.Equal([2, 3, 4], lp.GetBuffer());
        }

        [Fact]
        public void TestRounding()
        {
            var lp = new LowPassFilter(5);
            lp.Next(50);
            lp.Next(50);
            lp.Next(50);
            lp.Next(50);
            var last = lp.Next(50);

            Assert.Equal(50, last);
        }
    }
}
