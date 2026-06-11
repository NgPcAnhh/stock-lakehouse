"use client";

import { useEffect, useState } from "react";
import { AdminStatsCards } from "@/components/admin/AdminStatsCards";
import { AdminOverviewPanel } from "@/components/admin/AdminOverviewPanel";
import { AdminUserTable } from "@/components/admin/AdminUserTable";
import { AdminSessionsPanel } from "@/components/admin/AdminSessionsPanel";
import { AdminRolesPanel } from "@/components/admin/AdminRolesPanel";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent } from "@/components/ui/card";
import { fetchWithAuth } from "@/lib/auth";
import {
    ShieldCheck, Users, BarChart3, Monitor, Shield, RefreshCw,
    Database, LayoutDashboard, UserPlus, LogIn, Wifi, Search,
    AlertTriangle, TrendingUp, ChevronDown, ChevronUp, Activity, ShieldAlert
} from "lucide-react";
import { Button } from "@/components/ui/button";

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000/api/v1";

export function AdminSettingsContent() {
    const [stats, setStats] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
    const [isKpiExpanded, setIsKpiExpanded] = useState(false);
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
        <div className="space-y-6">
            {/* KPI Summary Box (Collapsible) */}
            {stats && !loading && (
                <Card className="shadow-sm border-border/50 overflow-hidden">
                    <div 
                        className="px-4 py-2 flex items-center justify-between cursor-pointer hover:bg-muted/50 transition-colors bg-muted/20"
                        onClick={() => setIsKpiExpanded(!isKpiExpanded)}
                    >
                        <div className="flex items-center gap-2">
                            <BarChart3 className="h-4 w-4 text-primary" />
                            <h3 className="text-xs font-semibold uppercase tracking-wider">Chỉ Số Tổng Quan</h3>
                            <Button 
                                variant="ghost" 
                                size="sm" 
                                onClick={(e) => {
                                    e.stopPropagation();
                                    loadStats();
                                }} 
                                disabled={loading} 
                                className="h-6 w-6 p-0 ml-2"
                            >
                                <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
                            </Button>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-[10px] text-muted-foreground italic">
                                {lastUpdated && `Cập nhật: ${lastUpdated.toLocaleTimeString("vi-VN")}`}
                            </span>
                            {isKpiExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                        </div>
                    </div>
                    
                    {isKpiExpanded && (
                        <CardContent className="pt-4 pb-4 px-4 space-y-4 bg-background">
                            {/* Row 1: Core Stats */}
                            <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
                                {[
                                    { label: "Tổng User", value: stats.total_users, icon: Users, color: "#3b82f6" },
                                    { label: "Mới 7 ngày", value: stats.new_users_7d, icon: UserPlus, color: "#22c55e" },
                                    { label: "Đăng nhập hôm nay", value: stats.logins_today, icon: LogIn, color: "#f59e0b" },
                                    { label: "Phiên hôm nay", value: stats.sessions_today, icon: Monitor, color: "#a855f7" },
                                    { label: "Active sessions", value: stats.active_sessions_count, icon: Wifi, color: "#10b981" },
                                    { label: "User bị khóa", value: stats.inactive_users, icon: Shield, color: "#ef4444" },
                                ].map(({ label, value, icon: Icon, color }) => (
                                    <div key={label} className="rounded-xl border border-border/50 px-3 py-2 flex items-center gap-2.5 bg-card">
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

                            {/* Row 2: Event Stats */}
                            <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
                                {[
                                    { label: "Click bài báo (7d)", value: stats.total_article_clicks_7d, icon: BarChart3, color: "#6366f1" },
                                    { label: "Click mã CK (7d)", value: stats.total_stock_clicks_7d, icon: TrendingUp, color: "#ec4899" },
                                    { label: "2FA kích hoạt", value: stats.totp_enabled_count, icon: ShieldCheck, color: "#f97316" },
                                    { label: "Phiên đang online", value: stats.active_sessions_count, icon: Wifi, color: "#10b981" },
                                    { label: "Tìm kiếm (7d)", value: stats.total_search_events_7d, icon: Search, color: "#0ea5e9" },
                                    { label: "Lỗi (30d)", value: stats.error_count_30d ?? 0, icon: AlertTriangle, color: "#ef4444" },
                                ].map(({ label, value, icon: Icon, color }) => (
                                    <div key={label} className="rounded-xl border border-border/50 px-3 py-2 flex items-center gap-2.5 bg-card">
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

            <Tabs defaultValue="overview" className="w-full">
                <TabsList className="flex flex-wrap h-auto p-1 gap-1 w-full justify-center mb-6 bg-muted/30 rounded-lg border border-border/50">
                    <TabsTrigger value="overview" className="flex gap-1.5 items-center text-xs px-3 py-1.5 data-[state=active]:bg-background data-[state=active]:shadow-sm">
                        <LayoutDashboard className="h-3.5 w-3.5" /> Tổng Quan
                    </TabsTrigger>
                    <TabsTrigger value="users" className="flex gap-1.5 items-center text-xs px-3 py-1.5 data-[state=active]:bg-background data-[state=active]:shadow-sm">
                        <Users className="h-3.5 w-3.5" /> Người Dùng
                    </TabsTrigger>

                    {/* External Links as Pseudo-tabs */}
                    <button 
                        onClick={() => window.open(`http://${hostname}:8080`, "_blank")}
                        className="inline-flex items-center justify-center whitespace-nowrap rounded-md px-3 py-1.5 text-xs font-medium ring-offset-background transition-all hover:bg-background/50 hover:text-foreground gap-1.5 text-muted-foreground"
                    >
                        <Database className="h-3.5 w-3.5" /> Dữ Liệu ↗
                    </button>

                    <button 
                        onClick={() => window.open(`http://${hostname}:8081`, "_blank")}
                        className="inline-flex items-center justify-center whitespace-nowrap rounded-md px-3 py-1.5 text-xs font-medium ring-offset-background transition-all hover:bg-background/50 hover:text-foreground gap-1.5 text-muted-foreground"
                    >
                        <Activity className="h-3.5 w-3.5" /> Bảng Điện ↗
                    </button>

                    <TabsTrigger value="sessions" className="flex gap-1.5 items-center text-xs px-3 py-1.5 data-[state=active]:bg-background data-[state=active]:shadow-sm">
                        <Monitor className="h-3.5 w-3.5" /> Phiên
                    </TabsTrigger>
                    <TabsTrigger value="roles" className="flex gap-1.5 items-center text-xs px-3 py-1.5 data-[state=active]:bg-background data-[state=active]:shadow-sm">
                        <Shield className="h-3.5 w-3.5" /> Vai Trò
                    </TabsTrigger>
                </TabsList>

                {/* ── Tab: Overview ── */}
                <TabsContent value="overview" className="space-y-6 mt-0">
                    {loading ? (
                        <div className="flex h-64 items-center justify-center bg-card rounded-xl border border-dashed border-border/50">
                            <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
                        </div>
                    ) : stats ? (
                        <AdminOverviewPanel stats={stats} />
                    ) : null}
                </TabsContent>

                {/* ── Tab: Users ── */}
                <TabsContent value="users" className="mt-0">
                    <div className="bg-card rounded-xl p-5 border border-border/50 shadow-sm">
                        <h3 className="text-sm font-bold mb-4 flex items-center gap-2">
                            <Users className="h-4 w-4" /> Quản Lý Tài Khoản Người Dùng
                        </h3>
                        <AdminUserTable />
                    </div>
                </TabsContent>

                {/* ── Tab: Sessions ── */}
                <TabsContent value="sessions" className="mt-0">
                    <div className="bg-card rounded-xl p-5 border border-border/50 shadow-sm">
                        <h3 className="text-sm font-bold mb-4 flex items-center gap-2">
                            <Monitor className="h-4 w-4" /> Quản Lý Phiên & Token
                        </h3>
                        <AdminSessionsPanel />
                    </div>
                </TabsContent>

                {/* ── Tab: Roles ── */}
                <TabsContent value="roles" className="mt-0">
                    <div className="bg-card rounded-xl p-5 border border-border/50 shadow-sm">
                        <h3 className="text-sm font-bold mb-4 flex items-center gap-2">
                            <Shield className="h-4 w-4" /> Quản Lý Vai Trò
                        </h3>
                        <AdminRolesPanel />
                    </div>
                </TabsContent>
            </Tabs>
        </div>
    );
}
