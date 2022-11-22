#运行500次2B，并统计成功次数。
path="tongji"

success=0;
fail=0;
echo "Start 2B test for 500 times";
touch str;
for ((i=1; i<=2; ++i))
do
    go test -run 2B | grep fail >> str
done

echo "500次运行"
echo "成功" $success "次"
echo "失败" $fail "次"