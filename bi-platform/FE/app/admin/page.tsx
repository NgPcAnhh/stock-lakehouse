"use client";

import { useEffect, useState } from "react";
import { AdminGuard } from "@/components/admin/AdminGuard";
import { AdminStatsCards } from "@/components/admin/AdminStatsCards";
import { AdminOverviewPanel } from "@/components/admin/AdminOverviewPanel";
import { AdminUserTable } from "@/components/admin/AdminUserTable";
import { AdminSessionsPanel } from "@/components/admin/AdminSessionsPanel";
import { AdminRolesPanel } from "@/components/admin/AdminRolesPanel";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { fetchWithAuth } from "@/lib/auth";
import {
    ShieldCheck, Users, BarChart3, Monitor, Shield, RefreshCw,
    Database, LayoutDashboard, UserPlus, LogIn, Wifi, Search,
    AlertTriangle, TrendingUp, ChevronDown, ChevronUp, Activity,
    Network, ExternalLink, Server, Workflow, Cpu, HardDrive
} from "lucide-react";
import { Button } from "@/components/ui/button";

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000/api/v1";

export default function AdminDashboardPage() {
    const [stats, setStats] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
    const [isKpiExpanded, setIsKpiExpanded] = useState(true);
    const [hostname, setHostname] = useState<string>("localhost");

    const loadStats = async () => {
        setLoading(true);
        try {
            const res = await fetchWithAuth(`${API}/admin/stats`);
            if (res.ok) {
                setStats(await res.json());
                setLastUpdated(new Date());
            }
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { 
        loadStats(); 
        if (typeof window !== "undefined") {
            setHostname(window.location.hostname);
        }
    }, []);

    return (
        <AdminGuard>
            <div className="min-h-screen bg-background">
                <div className="container mx-auto py-8 px-4 max-w-7xl space-y-6">

                    {/* Hero Header */}
                    <div className="flex items-start justify-between flex-wrap gap-4">
                        <div className="flex items-center gap-4">
                            <div className="p-3 rounded-2xl bg-gradient-to-br from-primary/20 to-primary/5 text-primary border border-primary/20">
                                <ShieldCheck className="h-8 w-8" />
                            </div>
                            <div>
                                <h1 className="text-3xl font-bold tracking-tight">Quản Trị Hệ Thống</h1>
                                <p className="text-muted-foreground mt-0.5">
                                    Admin Dashboard — Toàn bộ hoạt động hệ thống
                                </p>
                                {lastUpdated && (
                                    <p className="text-xs text-muted-foreground mt-1">
                                        Cập nhật lúc: {lastUpdated.toLocaleTimeString("vi-VN")}
                                    </p>
                                )}
                            </div>
                        </div>
                        <Button variant="outline" size="sm" onClick={loadStats} disabled={loading} className="gap-2">
                            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                            Làm mới
                        </Button>
                    </div>

                    {/* ═══ KPI Summary Box (Collapsible) ═══ */}
                    {stats && !loading && (
                        <Card className="shadow-sm border-border/50">
                            <div 
                                className="px-4 py-3 flex items-center justify-between cursor-pointer hover:bg-muted/50 transition-colors"
                                onClick={() => setIsKpiExpanded(!isKpiExpanded)}
                            >
                                <div className="flex items-center gap-2">
                                    <BarChart3 className="h-4 w-4 text-primary" />
                                    <h3 className="text-sm font-semibold">Chỉ Số Tổng Quan (KPIs)</h3>
                                </div>
                                <Button variant="ghost" size="sm" className="h-8 w-8 p-0">
                                    {isKpiExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                                </Button>
                            </div>
                            
                            {isKpiExpanded && (
                                <CardContent className="pt-0 pb-4 px-4 space-y-4">
                                    <div className="w-full h-px bg-border/50 mb-4" />
                                    {/* ═══ Row 1: 6 old stat cards (compact) ═══ */}
                                    <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
                                        {[
                                            { label: "Tổng User", value: stats.total_users, icon: Users, color: "#3b82f6" },
                                            { label: "Mới 7 ngày", value: stats.new_users_7d, icon: UserPlus, color: "#22c55e" },
                                            { label: "Đăng nhập hôm nay", value: stats.logins_today, icon: LogIn, color: "#f59e0b" },
                                            { label: "Phiên hôm nay", value: stats.sessions_today, icon: Monitor, color: "#a855f7" },
                                            { label: "Active sessions", value: stats.active_sessions_count, icon: Wifi, color: "#10b981" },
                                            { label: "User bị khóa", value: stats.inactive_users, icon: Shield, color: "#ef4444" },
                                        ].map(({ label, value, icon: Icon, color }) => (
                                            <div key={label} className="rounded-xl border border-border/50 px-3 py-2 flex items-center gap-2.5 bg-background">
                                                <div className="p-1.5 rounded-lg shrink-0" style={{ backgroundColor: `${color}18` }}>
                                                    <Icon className="h-3.5 w-3.5" style={{ color }} />
                                                </div>
                                                <div className="min-w-0">
                                                    <div className="text-base font-bold leading-tight">{value ?? 0}</div>
                                                    <div className="text-[10px] text-muted-foreground leading-tight truncate">{label}</div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>

                                    {/* ═══ Row 2: 6 new KPI cards (compact) ═══ */}
                                    <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
                                        {[
                                            { label: "Click bài báo (7 ngày)", value: stats.total_article_clicks_7d, icon: BarChart3, color: "#6366f1" },
                                            { label: "Click mã CK (7 ngày)", value: stats.total_stock_clicks_7d, icon: TrendingUp, color: "#ec4899" },
                                            { label: "2FA kích hoạt", value: stats.totp_enabled_count, icon: ShieldCheck, color: "#f97316" },
                                            { label: "Phiên đang online", value: stats.active_sessions_count, icon: Wifi, color: "#10b981" },
                                            { label: "Tìm kiếm (7 ngày)", value: stats.total_search_events_7d, icon: Search, color: "#0ea5e9" },
                                            { label: "Lỗi (30 ngày)", value: stats.error_count_30d ?? 0, icon: AlertTriangle, color: "#ef4444" },
                                        ].map(({ label, value, icon: Icon, color }) => (
                                            <div key={label} className="rounded-xl border border-border/50 px-3 py-2 flex items-center gap-2.5 bg-background">
                                                <div className="p-1.5 rounded-lg shrink-0" style={{ backgroundColor: `${color}18` }}>
                                                    <Icon className="h-3.5 w-3.5" style={{ color }} />
                                                </div>
                                                <div className="min-w-0">
                                                    <div className="text-base font-bold leading-tight">{value ?? 0}</div>
                                                    <div className="text-[10px] text-muted-foreground leading-tight truncate">{label}</div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </CardContent>
                            )}
                        </Card>
                    )}

                    {/* ═══ Tab Navigation ═══ */}
                    <Tabs defaultValue="overview" className="w-full">
                        <TabsList className="flex flex-wrap h-auto p-1 gap-1 w-full max-w-4xl mx-auto justify-center mb-6 bg-muted/50 rounded-lg">
                            <TabsTrigger value="overview" className="flex gap-1.5 items-center text-xs sm:text-sm px-3 py-1.5">
                                <LayoutDashboard className="h-3.5 w-3.5" /> Tổng Quan
                            </TabsTrigger>
                            <TabsTrigger value="users" className="flex gap-1.5 items-center text-xs sm:text-sm px-3 py-1.5">
                                <Users className="h-3.5 w-3.5" /> Người Dùng
                            </TabsTrigger>

                            <TabsTrigger value="data-pipeline" className="flex gap-1.5 items-center text-xs sm:text-sm px-3 py-1.5">
                                <Network className="h-3.5 w-3.5" /> Data Pipeline
                            </TabsTrigger>

                            <TabsTrigger value="sessions" className="flex gap-1.5 items-center text-xs sm:text-sm px-3 py-1.5">
                                <Monitor className="h-3.5 w-3.5" /> Phiên
                            </TabsTrigger>
                            <TabsTrigger value="roles" className="flex gap-1.5 items-center text-xs sm:text-sm px-3 py-1.5">
                                <Shield className="h-3.5 w-3.5" /> Vai Trò
                            </TabsTrigger>
                        </TabsList>

                        {/* ── Tab: Overview ── */}
                        <TabsContent value="overview" className="space-y-6">
                            {loading ? (
                                <div className="flex h-64 items-center justify-center">
                                    <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
                                </div>
                            ) : stats ? (
                                <AdminOverviewPanel stats={stats} />
                            ) : null}
                        </TabsContent>

                        {/* ── Tab: Users ── */}
                        <TabsContent value="users">
                            <div className="bg-card rounded-xl p-6 border border-border/50 shadow-sm">
                                <h2 className="text-xl font-bold mb-6 flex items-center gap-2">
                                    <Users className="h-5 w-5" /> Quản Lý Tài Khoản Người Dùng
                                </h2>
                                <AdminUserTable />
                            </div>
                        </TabsContent>

                        {/* ── Tab: Sessions ── */}
                        <TabsContent value="sessions">
                            <div className="bg-card rounded-xl p-6 border border-border/50 shadow-sm">
                                <h2 className="text-xl font-bold mb-6 flex items-center gap-2">
                                    <Monitor className="h-5 w-5" /> Quản Lý Phiên & Token
                                </h2>
                                <AdminSessionsPanel />
                            </div>
                        </TabsContent>

                        {/* ── Tab: Roles ── */}
                        <TabsContent value="roles">
                            <div className="bg-card rounded-xl p-6 border border-border/50 shadow-sm">
                                <h2 className="text-xl font-bold mb-6 flex items-center gap-2">
                                    <Shield className="h-5 w-5" /> Quản Lý Vai Trò
                                </h2>
                                <AdminRolesPanel />
                            </div>
                        </TabsContent>

                        {/* ── Tab: Data Pipeline ── */}
                        <TabsContent value="data-pipeline" className="space-y-6">
                            <div className="bg-card rounded-xl p-6 border border-border/50 shadow-sm">
                                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
                                    <div>
                                        <h2 className="text-xl font-bold flex items-center gap-2">
                                            <Network className="h-5 w-5 text-primary" /> Quản Lý Hạ Tầng Data Pipeline & Lakehouse
                                        </h2>
                                        <p className="text-sm text-muted-foreground mt-1">
                                            Giám sát các dịch vụ thu thập, xử lý và lưu trữ dữ liệu chứng khoán.
                                        </p>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <Badge variant="outline" className="bg-primary/5 text-primary border-primary/20 gap-1.5 px-3 py-1 text-xs">
                                            <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
                                            Cluster Active
                                        </Badge>
                                    </div>
                                </div>

                                <div className="grid gap-6 md:grid-cols-2">
                                    {/* Apache Airflow Card */}
                                    <Card className="hover:border-primary/30 transition-all duration-300 group">
                                        <CardContent className="p-6 flex flex-col justify-between h-full space-y-4">
                                            <div className="space-y-3">
                                                <div className="flex items-start justify-between">
                                                    <div className="p-2.5 rounded-xl bg-gradient-to-br from-blue-500/10 to-blue-500/5 text-blue-500 border border-blue-500/20 group-hover:from-blue-500/20">
                                                        <Workflow className="h-6 w-6" />
                                                    </div>
                                                    <Badge variant="secondary" className="text-[10px] font-mono">Port 8080</Badge>
                                                </div>
                                                <div>
                                                    <h3 className="font-bold text-lg">Apache Airflow</h3>
                                                    <p className="text-sm text-muted-foreground mt-1 leading-relaxed">
                                                        Hệ thống lập lịch và điều phối (orchestration) toàn bộ luồng công việc ETL. Chịu trách nhiệm gọi API kích hoạt Spark jobs, crawl dữ liệu hàng ngày và đồng bộ dữ liệu vào Lakehouse.
                                                    </p>
                                                </div>
                                                <div className="bg-muted/50 rounded-lg p-2.5 text-xs font-mono space-y-1">
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Default User:</span> <span className="font-semibold">airflow</span></div>
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Default Pass:</span> <span className="font-semibold">airflow</span></div>
                                                </div>
                                            </div>
                                            <Button 
                                                onClick={() => window.open(`http://${hostname}:8080`, "_blank")} 
                                                className="w-full mt-auto gap-2"
                                            >
                                                Truy cập Airflow Webserver <ExternalLink className="h-3.5 w-3.5" />
                                            </Button>
                                        </CardContent>
                                    </Card>

                                    {/* MinIO Console Card */}
                                    <Card className="hover:border-primary/30 transition-all duration-300 group">
                                        <CardContent className="p-6 flex flex-col justify-between h-full space-y-4">
                                            <div className="space-y-3">
                                                <div className="flex items-start justify-between">
                                                    <div className="p-2.5 rounded-xl bg-gradient-to-br from-amber-500/10 to-amber-500/5 text-amber-500 border border-amber-500/20 group-hover:from-amber-500/20">
                                                        <HardDrive className="h-6 w-6" />
                                                    </div>
                                                    <Badge variant="secondary" className="text-[10px] font-mono">Port 9001 (Console)</Badge>
                                                </div>
                                                <div>
                                                    <h3 className="font-bold text-lg">MinIO Object Storage</h3>
                                                    <p className="text-sm text-muted-foreground mt-1 leading-relaxed">
                                                        Hệ thống lưu trữ đối tượng tương thích S3 API. Đóng vai trò là Landing Zone cho dữ liệu thô (raw CSV/Parquet) và lưu trữ các bảng định dạng Apache Iceberg.
                                                    </p>
                                                </div>
                                                <div className="bg-muted/50 rounded-lg p-2.5 text-xs font-mono space-y-1">
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Access Key:</span> <span className="font-semibold">minioadmin</span></div>
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Secret Key:</span> <span className="font-semibold">minioadmin</span></div>
                                                </div>
                                            </div>
                                            <Button 
                                                onClick={() => window.open(`http://${hostname}:9001`, "_blank")} 
                                                className="w-full mt-auto gap-2"
                                                variant="outline"
                                            >
                                                Truy cập MinIO Console <ExternalLink className="h-3.5 w-3.5" />
                                            </Button>
                                        </CardContent>
                                    </Card>


                                    {/* Apache Spark Card */}
                                    <Card className="hover:border-primary/30 transition-all duration-300 group">
                                        <CardContent className="p-6 flex flex-col justify-between h-full space-y-4">
                                            <div className="space-y-3">
                                                <div className="flex items-start justify-between">
                                                    <div className="p-2.5 rounded-xl bg-gradient-to-br from-orange-500/10 to-orange-500/5 text-orange-500 border border-orange-500/20 group-hover:from-orange-500/20">
                                                        <Cpu className="h-6 w-6" />
                                                    </div>
                                                    <Badge variant="secondary" className="text-[10px] font-mono">Port 8082 (Master)</Badge>
                                                </div>
                                                <div>
                                                    <h3 className="font-bold text-lg">Apache Spark</h3>
                                                    <p className="text-sm text-muted-foreground mt-1 leading-relaxed">
                                                        Hệ thống tính toán phân tán. Đọc dữ liệu từ MinIO Landing Zone, làm sạch, biến đổi dữ liệu thô và ghi định dạng bảng Apache Iceberg.
                                                    </p>
                                                </div>
                                                <div className="bg-muted/50 rounded-lg p-2.5 text-xs font-mono space-y-1">
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Spark Master URL:</span> <span className="font-semibold">spark://spark-master:7077</span></div>
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Spark Workers:</span> <span className="font-semibold">1 Active</span></div>
                                                </div>
                                            </div>
                                            <Button 
                                                onClick={() => window.open(`http://${hostname}:8082`, "_blank")} 
                                                className="w-full mt-auto gap-2"
                                                variant="outline"
                                            >
                                                Truy cập Spark Master UI <ExternalLink className="h-3.5 w-3.5" />
                                            </Button>
                                        </CardContent>
                                    </Card>
                                    
                                    {/* ClickHouse Card */}
                                    <Card className="hover:border-primary/30 transition-all duration-300 group md:col-span-2">
                                        <CardContent className="p-6 flex flex-col md:flex-row md:items-center justify-between gap-6">
                                            <div className="space-y-3 flex-1">
                                                <div className="flex items-center gap-3">
                                                    <div className="p-2.5 rounded-xl bg-gradient-to-br from-yellow-500/10 to-yellow-500/5 text-yellow-500 border border-yellow-500/20 group-hover:from-yellow-500/20">
                                                        <Database className="h-6 w-6" />
                                                    </div>
                                                    <div>
                                                        <h3 className="font-bold text-lg">ClickHouse Data Warehouse</h3>
                                                        <p className="text-xs text-muted-foreground font-mono mt-0.5">Port HTTP: 8123 | Port TCP: 9004</p>
                                                    </div>
                                                </div>
                                                <p className="text-sm text-muted-foreground leading-relaxed">
                                                    Cơ sở dữ liệu dạng cột (OLAP) phục vụ phân tích dữ liệu lớn với độ trễ cực thấp. Tích hợp trực tiếp với Datalake MinIO thông qua Apache Iceberg Engine để đọc dữ liệu tự động mà không cần đồng bộ thủ công.
                                                </p>
                                            </div>
                                            <div className="flex flex-col sm:flex-row gap-3 sm:shrink-0">
                                                <div className="bg-muted/50 rounded-lg p-3 text-xs font-mono space-y-1 sm:w-56">
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Default User:</span> <span className="font-semibold">default</span></div>
                                                    <div className="flex justify-between"><span className="text-muted-foreground">Default Pass:</span> <span className="font-semibold">default</span></div>
                                                </div>
                                                <Button 
                                                    onClick={() => window.open(`http://${hostname}:8123/play`, "_blank")} 
                                                    className="gap-2 sm:self-center"
                                                    variant="outline"
                                                >
                                                    Mở ClickHouse Play <ExternalLink className="h-3.5 w-3.5" />
                                                </Button>
                                            </div>
                                        </CardContent>
                                    </Card>
                                </div>
                            </div>
                        </TabsContent>
                    </Tabs>
                </div>
            </div>
        </AdminGuard>
    );
}
